# Part 4: Persistence & Crash Recovery

**Goal:** Survive process restarts without losing committed data or violating safety. Persist Raft state and log to disk before responding.

**Time:** ~1 weekend

**Raft paper:** §5.2 (persistent state), §5.6, §5.7–5.8

---

## 4.1 What must be persisted

From Figure 2, persist **before responding** to RPCs or client requests:

| Field | Why |
|-------|-----|
| `currentTerm` | Must not vote in old term after restart |
| `votedFor` | Must not double-vote after restart |
| `log[]` | Must not lose committed entries |

Volatile state (`commitIndex`, `lastApplied`, leader state) is rebuilt after restart.

---

## 4.2 Step 1 — Persistence interface

Create `internal/persist/store.go`:

```go
package persist

import "github.com/YOUR_USERNAME/raft-kv-store/internal/rpc"

// State is the durable Raft snapshot.
type State struct {
    CurrentTerm int              `json:"current_term"`
    VotedFor    int              `json:"voted_for"`
    Log         []rpc.LogEntry   `json:"log"`
}

type Store interface {
    Save(state State) error
    Load() (State, error)
}
```

---

## 4.3 Step 2 — File-backed implementation

Create `internal/persist/file.go`:

```go
package persist

import (
    "encoding/json"
    "os"
    "path/filepath"
)

type FileStore struct {
    path string
}

func NewFileStore(dataDir string) (*FileStore, error) {
    if err := os.MkdirAll(dataDir, 0755); err != nil {
        return nil, err
    }
    return &FileStore{path: filepath.Join(dataDir, "raft_state.json")}, nil
}

func (f *FileStore) Save(state State) error {
    data, err := json.Marshal(state)
    if err != nil {
        return err
    }
    // Atomic write: temp file + rename
    tmp := f.path + ".tmp"
    if err := os.WriteFile(tmp, data, 0644); err != nil {
        return err
    }
    return os.Rename(tmp, f.path)
}

func (f *FileStore) Load() (State, error) {
    data, err := os.ReadFile(f.path)
    if os.IsNotExist(err) {
        return State{VotedFor: -1, Log: []rpc.LogEntry{}}, nil
    }
    if err != nil {
        return State{}, err
    }
    var state State
    if err := json.Unmarshal(data, &state); err != nil {
        return State{}, err
    }
    return state, nil
}
```

### Why atomic rename?

If the process crashes mid-write, you don't want a half-written JSON file. Write to `.tmp`, then `rename` (atomic on POSIX).

---

## 4.4 Step 3 — Integrate persistence into Raft

Add to `Node`:

```go
type Node struct {
    // ...
    persist persist.Store
}

func (n *Node) persistState() error {
    return n.persist.Save(persist.State{
        CurrentTerm: n.currentTerm,
        VotedFor:    n.votedFor,
        Log:         n.log,
    })
}
```

Call `persistState()` **before** responding in:

- `HandleRequestVote` (after granting vote or updating term)
- `HandleAppendEntries` (after appending entries)
- `SubmitCommand` / leader append (after appending to log)

Example in RequestVote:

```go
if reply.VoteGranted {
    n.votedFor = args.CandidateID
    if err := n.persistState(); err != nil {
        // log error; in production you'd handle more carefully
    }
    reply.VoteGranted = true
}
```

---

## 4.5 Step 4 — Restore on startup

```go
func NewNode(id int, peers []int, store persist.Store, transport Transport) (*Node, error) {
    state, err := store.Load()
    if err != nil {
        return nil, err
    }

    n := &Node{
        id:          id,
        peers:       peers,
        currentTerm: state.CurrentTerm,
        votedFor:    state.VotedFor,
        log:         state.Log,
        persist:     store,
        // ...
    }

    // Recompute commitIndex: find highest committed entry
    // On restart, leader will tell us via LeaderCommit in AppendEntries
    // For single-node recovery tests, scan log for known commit markers

    return n, nil
}
```

After restart, a node starts as Follower with restored log. The leader's next AppendEntries updates `commitIndex`.

---

## 4.6 Step 5 — Rebuild state machine from log

On startup, replay committed entries:

```go
func (n *Node) replayLog(store *kv.Store) {
    n.mu.Lock()
    defer n.mu.Unlock()

    for i := 1; i <= n.commitIndex; i++ {
        entry := n.log[i-1]
        store.Apply(entry.Command)
        n.lastApplied = i
    }
}
```

**Problem:** After crash, `commitIndex` may not be persisted (volatile). 

**Solution (standard):** 
- Leader's `LeaderCommit` in AppendEntries tells followers what's committed
- On lone restart, `commitIndex = 0` until leader contact — uncommitted entries aren't applied
- Alternative: persist `commitIndex` too (allowed, simplifies recovery)

For this project, **persist commitIndex** as well — it's pragmatic and common in implementations:

```go
type State struct {
    CurrentTerm int
    VotedFor    int
    Log         []rpc.LogEntry
    CommitIndex int  // add this
}
```

---

## 4.7 Step 6 — Election restriction (Figure 8)

You implemented `logIsUpToDate` in Part 2. After persistence, verify:

**Test scenario:**
1. 3 nodes, commit entries 1–5
2. Kill all nodes
3. Restart nodes 1 and 2 only (node 3 has shorter log on disk)
4. Node 3 must **not** become leader
5. Node with longer log wins election

```go
func TestElectionRestriction(t *testing.T) {
    // ... setup cluster, commit 5 entries
    // wipe node 3's data dir, restart with truncated log
    // assert node 3 never wins election while 1 or 2 alive
}
```

---

## 4.8 Crash recovery test

```go
func TestCrashRecovery(t *testing.T) {
    dir := t.TempDir()
    cluster := newTestClusterWithPersist(t, 3, dir)

    leader := cluster.waitLeader()
    leader.submit(SET("k", "v"))
    cluster.waitCommitted(1)

    // Simulate crash: stop node 1, wipe its memory, reload from disk
    node1 := cluster.nodes[0]
    node1.Stop()
    node1 = restartNodeFromDisk(t, node1.ID, dir)

    cluster.waitAppliedOnAll(1)
    val, found := node1.store.Get("k")
    assert.True(t, found)
    assert.Equal(t, "v", val)
}
```

---

## 4.9 Performance note (optional)

JSON rewrite on every append is slow. Optimizations (do later):

- **WAL append-only:** append one entry per write, periodic snapshot
- **Batch fsync:** group persists (careful with safety)

For learning, JSON is fine. Mention in README that production would use WAL + snapshot.

---

## 4.10 Safety checklist

Before Part 5, verify:

- [ ] Term and votedFor survive restart
- [ ] Log entries survive restart
- [ ] Uncommitted entries aren't applied after solo restart
- [ ] Node with stale log can't win election
- [ ] No committed entry is ever lost (test with majority restarts)

---

## Checkpoint ✓

- [ ] `TestCrashRecovery` passes
- [ ] `TestElectionRestriction` passes
- [ ] Each node has a `data/node-N/` directory with state file
- [ ] You can explain what's persisted vs volatile

---

## Next

→ **[Part 5: Chaos Testing & Linearizability](05-chaos-testing.md)**

This is where you prove the implementation actually works — not just "looks right."
