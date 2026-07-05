# Part 5: Chaos Testing & Linearizability

**Goal:** Prove your Raft implementation is correct under failures — not just in the happy path.

**Time:** 1–2 weekends

**You'll learn:** Network partitions in tests, deterministic simulation, linearizability checking, Jepsen-style thinking.

---

## 5.1 Why chaos testing matters

Raft implementations often "look done" but fail when:

- Messages are duplicated
- Messages arrive out of order
- Two partitions both think they have a leader
- A slow node rejoins with a stale log

**Linearizability** means: every operation appears to happen atomically at some point between its start and end, consistent with a sequential history. If your KV store is linearizable, clients never see impossible orderings (e.g. read returns old value after a successful write).

Tools like Jepsen do this for production databases. You'll build a **mini version** in Go tests.

---

## 5.2 Step 1 — Simulated network

Create `internal/transport/simulator.go`:

```go
package transport

import (
    "sync"
    "time"

    "github.com/YOUR_USERNAME/raft-kv-store/internal/rpc"
)

// SimNetwork wraps deliveries between nodes with configurable faults.
type SimNetwork struct {
    mu sync.Mutex

    nodes       map[int]rpcReceiver
    partitions  [][]int // disjoint groups that can't talk across groups
    dropRate    float64 // 0.0 - 1.0
    delayMin    time.Duration
    delayMax    time.Duration
    duplicate   bool
    disabled    map[int]bool // "dead" nodes
}

type rpcReceiver interface {
    HandleRequestVote(args rpc.RequestVoteArgs) rpc.RequestVoteReply
    HandleAppendEntries(args rpc.AppendEntriesArgs) rpc.AppendEntriesReply
}

func NewSimNetwork() *SimNetwork {
    return &SimNetwork{
        nodes:    make(map[int]rpcReceiver),
        disabled: make(map[int]bool),
    }
}

func (s *SimNetwork) Register(id int, r rpcReceiver) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.nodes[id] = r
}

func (s *SimNetwork) Kill(id int)  { s.disabled[id] = true }
func (s *SimNetwork) Revive(id int) { delete(s.disabled, id) }

func (s *SimNetwork) Partition(groups ...[]int) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.partitions = groups
}

func (s *SimNetwork) Heal() {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.partitions = nil
}

func (s *SimNetwork) canDeliver(from, to int) bool {
    if s.disabled[from] || s.disabled[to] {
        return false
    }
    if len(s.partitions) == 0 {
        return true
    }
    fromGroup, toGroup := -1, -1
    for i, g := range s.partitions {
        for _, id := range g {
            if id == from { fromGroup = i }
            if id == to { toGroup = i }
        }
    }
    return fromGroup == toGroup
}
```

Implement `DeliverAppendEntries(from, to, args)` that:
1. Checks `canDeliver`
2. Optionally drops (random < dropRate)
3. Optionally delays in goroutine
4. Optionally delivers twice if duplicate
5. Calls target handler

---

## 5.3 Step 2 — Test cluster harness

Create `internal/raft/testcluster.go`:

```go
type TestCluster struct {
    t     *testing.T
    net   *transport.SimNetwork
    nodes []*TestNode
}

type TestNode struct {
    ID     int
    Node   *Node
    Store  *kv.Store
    Cancel context.CancelFunc
}

func NewTestCluster(t *testing.T, n int) *TestCluster {
    net := transport.NewSimNetwork()
    // create n nodes, register with net, start Run() and applyLoop
    // return cluster
}

func (c *TestCluster) WaitLeader(timeout time.Duration) *TestNode { /* ... */ }

func (c *TestCluster) Submit(nodeID int, cmd kv.Command) (kv.ApplyResult, error) { /* ... */ }

func (c *TestCluster) Partition(a, b []int) { c.net.Partition(a, b) }

func (c *TestCluster) Heal() { c.net.Heal() }

func (c *TestCluster) Kill(id int) { /* stop + net.Kill */ }

func (c *TestCluster) Revive(id int) { /* restart from persist + net.Revive */ }
```

This harness becomes your primary development tool.

---

## 5.4 Step 3 — Linearizability checker (simplified)

For a KV store with GET/SET, use a **sequential specification**: a golden map updated in order.

Create `internal/raft/linearizability.go`:

```go
package raft

import (
    "sync"
    "testing"
)

type OpRecord struct {
    ClientID  string
    Op        string
    Key       string
    Value     string
    StartTime time.Time
    EndTime   time.Time
    Result    kv.ApplyResult
    OK        bool
}

type History struct {
    mu   sync.Mutex
    ops  []OpRecord
}

func (h *History) Record(op OpRecord) {
    h.mu.Lock()
    defer h.mu.Unlock()
    h.ops = append(h.ops, op)
}

// CheckLinearizable verifies there exists a sequential ordering of ops
// consistent with real-time ordering and the KV semantics.
func CheckLinearizable(t *testing.T, ops []OpRecord) {
    // Sort by start time for hint, try permutations of concurrent ops
    // For small tests: brute-force check all valid orderings
    // Reference: "Linearizability: A Correctness Condition for Concurrent Objects"
    // Practical approach: use github.com/anishathalye/porcupine (optional stretch)
}
```

### Practical approach for this project

Start with **specific scenarios** (not full permutation checker):

1. **Write monotonicity:** After successful SET k=v, all subsequent GETs return v (until DELETE)
2. **No lost writes:** N concurrent SETs to different keys — all appear in final state
3. **Partition test:** Minority partition can't commit; majority can; after heal, minority catches up

Full Porcupine integration is a stretch goal — it models history as a graph.

---

## 5.5 Step 4 — Chaos scenarios (implement these tests)

Create `internal/raft/chaos_test.go`:

### Scenario 1: Random leader kills

```go
func TestChaosRandomLeaderKill(t *testing.T) {
    cluster := NewTestCluster(t, 3)
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    go func() {
        for ctx.Err() == nil {
            time.Sleep(randomDuration(100, 500*time.Millisecond))
            if leader := cluster.CurrentLeader(); leader != nil {
                cluster.Kill(leader.ID)
                time.Sleep(randomDuration(200, 800*time.Millisecond))
                cluster.Revive(leader.ID)
            }
        }
    }()

    // Concurrent writers
    var wg sync.WaitGroup
    for i := 0; i < 3; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for j := 0; j < 50; j++ {
                key := fmt.Sprintf("k%d", id)
                cluster.SubmitToAny(SET(key, fmt.Sprintf("v%d", j)))
            }
        }(i)
    }
    wg.Wait()
    cancel()

    // Verify final state is self-consistent
    cluster.AssertConsistentState(t)
}
```

### Scenario 2: Network partition

```go
func TestChaosPartition(t *testing.T) {
    cluster := NewTestCluster(t, 5)

    // Partition: {1,2,3} | {4,5} — minority can't write
    cluster.Partition([]int{1, 2, 3}, []int{4, 5})

    leader := cluster.WaitLeader(2 * time.Second)
    cluster.Submit(leader.ID, SET("x", "1")) // should succeed on majority side

    _, err := cluster.SubmitToMinority(DELETE("x"))
    assert.Error(t, err) // minority can't commit

    cluster.Heal()
    cluster.WaitConverged(5 * time.Second)

    val, found := cluster.nodes[4].Store.Get("x")
    assert.True(t, found)
    assert.Equal(t, "1", val)
}
```

### Scenario 3: Message duplication

```go
func TestChaosDuplicateMessages(t *testing.T) {
    net := transport.NewSimNetwork()
    net.SetDuplicate(true)
    // ... ensure idempotent handling — same AppendEntries twice is OK
}
```

### Scenario 4: Slow follower

Delay messages to one follower by 500ms. Submit 100 writes. Assert follower eventually catches up and matches leader.

---

## 5.6 Step 5 — `AssertConsistentState`

```go
func (c *TestCluster) AssertConsistentState(t *testing.T) {
    t.Helper()
    var reference map[string]string
    for i, node := range c.nodes {
        snap := node.Store.Snapshot()
        if i == 0 {
            reference = snap
            continue
        }
        if !mapsEqual(reference, snap) {
            t.Fatalf("node %d state differs from node 0\nref: %v\ngot: %v",
                node.ID, reference, snap)
        }
    }
}
```

All **alive** nodes must have identical committed state.

---

## 5.7 Step 6 — Run chaos tests in CI

Add to `.github/workflows/test.yml`:

```yaml
name: test
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.22'
      - run: go test -race -count=5 ./internal/raft/ -run Chaos -timeout 120s
      - run: go test -race ./...
```

Running chaos tests `-count=5` catches flaky timing bugs.

---

## 5.8 When tests fail (debug playbook)

1. **Capture logs** from all nodes with synchronized timestamps
2. **Dump logs** at failure: each node's log entries with term/index
3. **Check term** — is a stale leader still serving?
4. **Check commitIndex** on each node — did minority apply uncommitted?
5. **Reduce chaos** — remove duplication, then delay, then partition
6. **Add invariant assertions** inside Raft (debug builds):

```go
// after every mutation
if n.commitIndex > len(n.log) { panic("commitIndex > log len") }
if n.lastApplied > n.commitIndex { panic("applied uncommitted") }
```

Document every failure in `BUGS.md`.

---

## 5.9 Stretch goals

- Integrate [Porcupine](https://github.com/anishathalye/porcupine) for automatic linearizability
- 1000-op random op fuzz test (`testing/quick`)
- Simulated clock for deterministic timeouts

---

## Checkpoint ✓

- [ ] `TestChaosRandomLeaderKill` passes `-count=5`
- [ ] `TestChaosPartition` passes
- [ ] All alive nodes have identical state after heal
- [ ] At least 3 bugs documented in `BUGS.md` with root causes
- [ ] You can explain linearizability in an interview

---

## Next

→ **[Part 6: Polish & Write-Up](06-polish-and-writeup.md)**

CLI improvements, architecture README, metrics, and preparing to talk about this in interviews.
