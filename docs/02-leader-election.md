# Part 2: Leader Election

**Goal:** Multiple nodes elect exactly one leader. Followers detect leader failure and re-elect. No client writes through Raft yet — no log replication.

**Time:** ~1 weekend

**Raft paper:** §5.2, Figure 2

**You'll learn:** Terms, randomized timeouts, split votes, RequestVote RPC, concurrent state machine with goroutines.

---

## 2.1 What success looks like

Start 3 nodes. Logs should show something like:

```
node-1: became candidate term=1
node-1: became leader term=1
node-2: following leader node-1 term=1
node-3: following leader node-1 term=1
```

Kill the leader process. Within ~1 second, a new leader appears with a higher term.

---

## 2.2 Step 1 — The Raft `Node` struct

Create `internal/raft/node.go`. This is the heart of the project.

```go
package raft

import (
    "sync"
    "time"

    "github.com/YOUR_USERNAME/raft-kv-store/internal/rpc"
)

type State int

const (
    Follower State = iota
    Candidate
    Leader
)

type Node struct {
    mu sync.Mutex

    id    int
    peers []int          // IDs of other nodes
    state State

    // Persistent state (Figure 2) — in-memory for now, Part 4 adds disk
    currentTerm int
    votedFor    int   // -1 if none
    log         []rpc.LogEntry

    // Volatile state
    commitIndex int
    lastApplied int

    // Leader volatile state
    nextIndex  map[int]int
    matchIndex map[int]int

    // Channels / timers
    electionReset chan struct{}
    applyCh       chan rpc.LogEntry // commands to apply to state machine

    // Transport sends RPCs to other nodes (you'll implement in Step 3)
    transport Transport
}

type Transport interface {
    RequestVote(targetID int, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error)
    AppendEntries(targetID int, args rpc.AppendEntriesArgs) (rpc.AppendEntriesReply, error)
    BroadcastRequestVote(args rpc.RequestVoteArgs) []rpc.RequestVoteReply
}
```

Constants:

```go
const (
    noLeader   = -1
    electionTimeoutMin = 150 * time.Millisecond
    electionTimeoutMax = 300 * time.Millisecond
    heartbeatInterval    = 50 * time.Millisecond
)
```

### Key insight: one mutex

All Raft state mutations happen with `mu` held. RPC handlers acquire `mu`, update state, release. The election timer goroutine resets on heartbeat. **Draw a diagram** of which goroutines touch `mu`.

---

## 2.3 Step 2 — Election timer goroutine

Create `internal/raft/election.go`:

```go
func (n *Node) Run() {
    go n.electionLoop()
    // heartbeatLoop added in Part 3 when you're Leader
}

func (n *Node) electionLoop() {
    timeout := randomElectionTimeout()
    timer := time.NewTimer(timeout)
    defer timer.Stop()

    for {
        select {
        case <-n.electionReset:
            if !timer.Stop() {
                <-timer.C
            }
            timer.Reset(randomElectionTimeout())
        case <-timer.C:
            n.mu.Lock()
            if n.state != Leader {
                n.startElection()
            }
            n.mu.Unlock()
            timer.Reset(randomElectionTimeout())
        }
    }
}

func randomElectionTimeout() time.Duration {
    delta := electionTimeoutMax - electionTimeoutMin
    return electionTimeoutMin + time.Duration(rand.Int63n(int64(delta)))
}
```

Add `"math/rand"` import. Seed rand in `main` or init.

### Why randomized timeout?

If all nodes timeout simultaneously, they all become candidates and **split the vote** — no one gets a majority. Random 150–300ms makes this unlikely.

---

## 2.4 Step 3 — `startElection`

Still in `election.go`:

```go
func (n *Node) startElection() {
    n.state = Candidate
    n.currentTerm++
    n.votedFor = n.id
    term := n.currentTerm
    lastIdx, lastTerm := n.lastLogInfo()

    args := rpc.RequestVoteArgs{
        Term:         term,
        CandidateID:  n.id,
        LastLogIndex: lastIdx,
        LastLogTerm:  lastTerm,
    }

    // Vote for self
    votes := 1
    majority := len(n.peers)/2 + 1 // includes self; adjust if peers excludes self

    replies := n.transport.BroadcastRequestVote(args)
    for _, reply := range replies {
        if reply.Term > term {
            n.becomeFollower(reply.Term)
            return
        }
        if reply.VoteGranted {
            votes++
        }
    }

    if votes >= majority && n.state == Candidate && n.currentTerm == term {
        n.becomeLeader()
    }
}

func (n *Node) becomeFollower(term int) {
    n.state = Follower
    n.currentTerm = term
    n.votedFor = noLeader
}

func (n *Node) becomeLeader() {
    n.state = Leader
    // Initialize leader state (Part 3 uses these for replication)
    for _, peer := range n.peers {
        n.nextIndex[peer] = len(n.log) + 1
        n.matchIndex[peer] = 0
    }
    // heartbeatLoop starts in Part 3
}
```

**Important:** `startElection` is called with `mu` already held.

Implement `lastLogInfo()`:

```go
func (n *Node) lastLogInfo() (index, term int) {
    if len(n.log) == 0 {
        return 0, 0
    }
    last := n.log[len(n.log)-1]
    return last.Index, last.Term
}
```

---

## 2.5 Step 4 — `RequestVote` RPC handler

Create `internal/raft/rpc_handlers.go`:

```go
func (n *Node) HandleRequestVote(args rpc.RequestVoteArgs) rpc.RequestVoteReply {
    n.mu.Lock()
    defer n.mu.Unlock()

    reply := rpc.RequestVoteReply{Term: n.currentTerm}

    // Rule 1: Reply false if term < currentTerm
    if args.Term < n.currentTerm {
        reply.VoteGranted = false
        return reply
    }

    // Rule 2: If term > currentTerm, become follower
    if args.Term > n.currentTerm {
        n.becomeFollower(args.Term)
    }

    // Rule 3: Grant vote if haven't voted this term AND log is up-to-date
    if (n.votedFor == noLeader || n.votedFor == args.CandidateID) &&
        n.logIsUpToDate(args.LastLogIndex, args.LastLogTerm) {
        n.votedFor = args.CandidateID
        reply.VoteGranted = true
        n.resetElectionTimer()
    }

    return reply
}

func (n *Node) resetElectionTimer() {
    select {
    case n.electionReset <- struct{}{}:
    default:
    }
}
```

Implement `logIsUpToDate` (Figure 2):

```go
func (n *Node) logIsUpToDate(candidateLastIndex, candidateLastTerm int) bool {
    lastIndex, lastTerm := n.lastLogInfo()
    if candidateLastTerm != lastTerm {
        return candidateLastTerm > lastTerm
    }
    return candidateLastIndex >= lastIndex
}
```

---

## 2.6 Step 5 — In-process transport (for testing)

Before real TCP between nodes, build an **in-memory transport** so you can test elections without network flakiness.

Create `internal/transport/memory.go`:

```go
package transport

import (
    "sync"

    "github.com/YOUR_USERNAME/raft-kv-store/internal/rpc"
    "github.com/YOUR_USERNAME/raft-kv-store/internal/raft"
)

// MemoryNetwork connects in-process Raft nodes.
type MemoryNetwork struct {
    mu    sync.Mutex
    nodes map[int]raft.RPCReceiver // interface with HandleRequestVote, etc.
}

func NewMemoryNetwork() *MemoryNetwork {
    return &MemoryNetwork{nodes: make(map[int]raft.RPCReceiver)}
}

func (net *MemoryNetwork) Register(id int, node raft.RPCReceiver) {
    net.mu.Lock()
    defer net.mu.Unlock()
    net.nodes[id] = node
}

// Implement raft.Transport by calling the target node's handler directly.
```

Define `raft.RPCReceiver` interface in `internal/raft/node.go`:

```go
type RPCReceiver interface {
    HandleRequestVote(args rpc.RequestVoteArgs) rpc.RequestVoteReply
    HandleAppendEntries(args rpc.AppendEntriesArgs) rpc.AppendEntriesReply
}
```

This pattern lets Part 5's chaos network wrap the same interface.

---

## 2.7 Step 6 — Wire nodes together

Update `cmd/server/main.go` to accept:

```bash
go run ./cmd/server -id 1 -peers 1,2,3 -addr :9001
go run ./cmd/server -id 2 -peers 1,2,3 -addr :9002
go run ./cmd/server -id 3 -peers 1,2,3 -addr :9003
```

For now, use TCP transport between nodes (implement `internal/transport/tcp.go`) OR start with memory network in tests only.

**Suggested approach:** Get election working in **tests with MemoryNetwork first**, then add TCP.

---

## 2.8 Step 7 — Heartbeat as empty AppendEntries (preview)

Raft leaders send periodic **AppendEntries** RPCs (even with zero entries) to tell followers "I'm alive." Followers reset their election timer on any valid AppendEntries.

In Part 2, implement a minimal `HandleAppendEntries`:

```go
func (n *Node) HandleAppendEntries(args rpc.AppendEntriesArgs) rpc.AppendEntriesReply {
    n.mu.Lock()
    defer n.mu.Unlock()

    reply := rpc.AppendEntriesReply{Term: n.currentTerm}

    if args.Term < n.currentTerm {
        return reply
    }

    if args.Term > n.currentTerm || n.state != Follower {
        n.becomeFollower(args.Term)
    }

    n.resetElectionTimer()
    reply.Success = true
    // Ignore entries in Part 2 — Part 3 implements replication
    return reply
}
```

Leader sends empty AppendEntries every `heartbeatInterval` — add `heartbeatLoop` in Part 3, but you can stub it now to reduce election churn during testing.

---

## 2.9 Tests you must write

Create `internal/raft/election_test.go`:

### Test 1: Single leader elected

```go
func TestElectsSingleLeader(t *testing.T) {
    net := transport.NewMemoryNetwork()
    nodes := startCluster(t, net, 3)

    leader := waitForLeader(t, nodes, 2*time.Second)
    if leader == nil {
        t.Fatal("no leader elected")
    }

    // Exactly one leader
    count := 0
    for _, n := range nodes {
        if n.State() == raft.Leader {
            count++
        }
    }
    if count != 1 {
        t.Fatalf("want 1 leader, got %d", count)
    }
}
```

### Test 2: Re-election after leader failure

```go
func TestReElectAfterLeaderFailure(t *testing.T) {
    net := transport.NewMemoryNetwork()
    nodes := startCluster(t, net, 3)

    leader1 := waitForLeader(t, nodes, 2*time.Second)
    leader1.Stop() // disconnect from network

    leader2 := waitForLeader(t, nodes, 3*time.Second)
    if leader2 == nil {
        t.Fatal("no new leader")
    }
    if leader2.ID() == leader1.ID() {
        t.Fatal("same node re-elected immediately without need")
    }
}
```

### Test 3: No split brain (same term)

After election settles, all nodes should agree on `currentTerm`.

Run with race detector:

```bash
go test -race ./internal/raft/ -run TestElect -v -count=1
```

---

## 2.10 Debugging tips (you will need these)

| Symptom | Likely cause |
|---------|--------------|
| No leader ever elected | Forgot to vote for self; wrong majority calculation |
| Leader flapping every 200ms | Heartbeats not resetting election timer |
| All nodes always candidates | Split votes — widen timeout range or check vote granting |
| Term stuck at 0 | Not incrementing term in `startElection` |
| Deadlock | Holding `mu` while calling `transport` which calls back into same node |

**Add structured logging:**

```go
log.Printf("[node-%d] term=%d state=%s event=election_started", n.id, n.currentTerm, stateString(n.state))
```

Run 3 terminals side by side and watch terms align.

---

## 2.11 Exercise: split vote scenario

1. Set `electionTimeoutMin = electionTimeoutMax = 200ms` (no randomness)
2. Run TestElectsSingleLeader 10 times
3. Observe failures — terms increment rapidly, no leader
4. Restore randomness — tests pass
5. Write up what happened in `BUGS.md`

This is a great interview story.

---

## Checkpoint ✓

- [ ] 3-node cluster elects exactly one leader
- [ ] Killing leader triggers re-election within ~1s
- [ ] `go test -race ./internal/raft/ -run TestElect` passes
- [ ] You can draw RequestVote flow from memory
- [ ] You understand why randomized timeouts matter

---

## Next

→ **[Part 3: Log Replication](03-log-replication.md)**

The leader will accept client writes, append to its log, replicate to followers, and commit on majority ack. This is where the KV store becomes **distributed**.
