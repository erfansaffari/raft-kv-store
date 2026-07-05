# Part 3: Log Replication

**Goal:** Client writes go through the leader, replicate to a majority, then apply to the KV store. Reads can be served by the leader (simple) or require quorum (stretch).

**Time:** 1–2 weekends

**Raft paper:** §5.3, Figures 3–6

**You'll learn:** AppendEntries RPC, commit index, apply loop, log consistency check, client redirect.

---

## 3.1 The write path (memorize this)

```
Client ──► Leader: append entry to local log (term T)
Leader ──► Followers: AppendEntries RPC with new entry
Followers ──► Leader: success
Leader: commitIndex = index (once majority acked)
Leader: apply entry to KV store
Leader ──► Client: OK
```

**Critical rule:** A entry is **committed** when the leader knows it exists on a majority. Only then does it apply to the state machine.

---

## 3.2 Step 1 — Client request handler on the leader

Create `internal/raft/client.go`:

```go
func (n *Node) SubmitCommand(cmd kv.Command) (kv.ApplyResult, error) {
    n.mu.Lock()
    if n.state != Leader {
        leaderID := n.currentLeaderID() // track known leader, or -1
        n.mu.Unlock()
        return kv.ApplyResult{}, &NotLeaderError{LeaderID: leaderID}
    }

    index := len(n.log) + 1
    entry := rpc.LogEntry{
        Term:    n.currentTerm,
        Index:   index,
        Command: cmd,
    }
    n.log = append(n.log, entry)
    n.mu.Unlock()

    // Wait until this index is committed and applied
    result, err := n.waitForApply(index)
    return result, err
}
```

You'll need:
- `applyNotify` channel or condition variable per pending index
- `applyLoop` goroutine that watches `commitIndex` and `lastApplied`

---

## 3.3 Step 2 — The apply loop

Create `internal/raft/apply.go`:

```go
func (n *Node) applyLoop(store *kv.Store) {
    for {
        n.mu.Lock()
        for n.lastApplied < n.commitIndex {
            n.lastApplied++
            entry := n.log[n.lastApplied-1] // logs are 1-indexed
            n.mu.Unlock()

            result := store.Apply(entry.Command)
            n.notifyApplied(n.lastApplied, result)

            n.mu.Lock()
        }
        n.mu.Unlock()
        time.Sleep(1 * time.Millisecond) // or use channel signal
    }
}
```

**Why separate apply loop?** Commit and apply are distinct: commit is a Raft decision; apply is state machine execution. Keeping them separate matches the paper and simplifies persistence later.

---

## 3.4 Step 3 — Full AppendEntries handler

Replace the Part 2 stub in `rpc_handlers.go`. This is the longest handler — go slowly.

```go
func (n *Node) HandleAppendEntries(args rpc.AppendEntriesArgs) rpc.AppendEntriesReply {
    n.mu.Lock()
    defer n.mu.Unlock()

    reply := rpc.AppendEntriesReply{Term: n.currentTerm}

    // 1. Reply false if term < currentTerm
    if args.Term < n.currentTerm {
        return reply
    }

    // 2. Convert to follower if needed
    if args.Term > n.currentTerm || n.state != Follower {
        n.becomeFollower(args.Term)
    }
    n.resetElectionTimer()

    // 3. Log consistency check (Figure 2)
    if args.PrevLogIndex > 0 {
        if args.PrevLogIndex > len(n.log) {
            reply.ConflictIndex = len(n.log) + 1
            return reply
        }
        prev := n.log[args.PrevLogIndex-1]
        if prev.Term != args.PrevLogTerm {
            reply.ConflictTerm = prev.Term
            // Find first index with ConflictTerm
            for i := args.PrevLogIndex; i >= 1; i-- {
                if n.log[i-1].Term != prev.Term {
                    reply.ConflictIndex = i + 1
                    break
                }
                if i == 1 {
                    reply.ConflictIndex = 1
                }
            }
            return reply
        }
    }

    // 4. Append new entries (overwrite conflicts)
    insertAt := args.PrevLogIndex
    for i, newEntry := range args.Entries {
        idx := insertAt + i + 1
        if idx <= len(n.log) {
            if n.log[idx-1].Term != newEntry.Term {
                n.log = n.log[:idx-1] // delete conflicting suffix
            }
        }
        if idx > len(n.log) {
            n.log = append(n.log, newEntry)
        }
    }

    // 5. Update commit index
    if args.LeaderCommit > n.commitIndex {
        n.commitIndex = min(args.LeaderCommit, len(n.log))
    }

    reply.Success = true
    return reply
}
```

Study **Figure 3** in the paper alongside this code. The conflict index optimization (Figure 7) saves round trips — implement the basic version first, optimize later.

---

## 3.5 Step 4 — Leader replication loop

Create `internal/raft/replication.go`:

```go
func (n *Node) heartbeatLoop() {
    ticker := time.NewTicker(heartbeatInterval)
    defer ticker.Stop()

    for range ticker.C {
        n.mu.Lock()
        if n.state != Leader {
            n.mu.Unlock()
            return // stopped being leader
        }
        n.replicateToAll()
        n.mu.Unlock()
    }
}

func (n *Node) replicateToAll() {
    for _, peer := range n.peers {
        if peer == n.id {
            continue
        }
        go n.replicateOnce(peer) // don't hold mu in network call
    }
}

func (n *Node) replicateOnce(peer int) {
    n.mu.Lock()
    if n.state != Leader {
        n.mu.Unlock()
        return
    }
    nextIdx := n.nextIndex[peer]
    prevLogIndex := nextIdx - 1
    prevLogTerm := 0
    if prevLogIndex > 0 {
        prevLogTerm = n.log[prevLogIndex-1].Term
    }
    entries := n.log[nextIdx-1:] // may be empty (heartbeat)
    args := rpc.AppendEntriesArgs{
        Term:         n.currentTerm,
        LeaderID:     n.id,
        PrevLogIndex: prevLogIndex,
        PrevLogTerm:  prevLogTerm,
        Entries:      entries,
        LeaderCommit: n.commitIndex,
    }
    n.mu.Unlock()

    reply, err := n.transport.AppendEntries(peer, args)
    if err != nil {
        return
    }

    n.mu.Lock()
    defer n.mu.Unlock()

    if reply.Term > n.currentTerm {
        n.becomeFollower(reply.Term)
        return
    }
    if n.state != Leader || args.Term != n.currentTerm {
        return
    }

    if reply.Success {
        if len(entries) > 0 {
            lastNew := entries[len(entries)-1].Index
            n.matchIndex[peer] = lastNew
            n.nextIndex[peer] = lastNew + 1
        }
        n.advanceCommitIndex()
    } else {
        // Decrement nextIndex and retry (back off optimistically with ConflictIndex in v2)
        if reply.ConflictIndex > 0 {
            n.nextIndex[peer] = reply.ConflictIndex
        } else {
            n.nextIndex[peer] = max(1, n.nextIndex[peer]-1)
        }
    }
}
```

---

## 3.6 Step 5 — Advance commit index (Figure 8 rule)

```go
func (n *Node) advanceCommitIndex() {
    // Only commit entries from current term (Figure 8.2)
    for idx := len(n.log); idx > n.commitIndex; idx-- {
        if n.log[idx-1].Term != n.currentTerm {
            continue
        }
        count := 1 // self
        for _, peer := range n.peers {
            if peer != n.id && n.matchIndex[peer] >= idx {
                count++
            }
        }
        majority := len(n.peers)/2 + 1
        if count >= majority {
            n.commitIndex = idx
            break
        }
    }
}
```

**Why only current term?** Figure 8.2 prevents committing entries from old terms that might not be on all future majorities. This is subtle — read the paper paragraph carefully.

---

## 3.7 Step 6 — Client redirect on followers

Update the server RPC handler:

```go
case rpc.MsgClientRequest:
    result, err := raftNode.SubmitCommand(req.Cmd)
    if errors.Is(err, ErrNotLeader) {
        _ = rpc.WriteMessage(conn, rpc.MsgClientResponse, rpc.ClientResponse{
            OK:       false,
            LeaderID: fmt.Sprintf("node-%d", err.LeaderID),
            Error:    "not leader",
        })
        continue
    }
    // ... success response
```

Client retries against suggested leader (or loops through all peers).

---

## 3.8 Tests you must write

Create `internal/raft/replication_test.go`:

### Test 1: Basic replication

```go
func TestBasicReplication(t *testing.T) {
    cluster := newTestCluster(t, 3)
    leader := cluster.waitLeader()

    result, err := leader.submit(SET("x", "1"))
    assert.NoError(t, err)

    cluster.waitAppliedOnAll(1)

    for _, node := range cluster.nodes {
        val, found := node.store.Get("x")
        assert.True(t, found)
        assert.Equal(t, "1", val)
    }
}
```

### Test 2: Follower catches up after partition heals

1. Partition follower 3 from leader
2. Submit 3 writes to leader
3. Heal partition
4. Assert follower 3 has same log length and same store state

### Test 3: Conflicting logs (Figure 7 scenario)

Manually inject divergent logs into two nodes, bring up leader, submit new write — assert logs converge.

Run:

```bash
go test -race ./internal/raft/ -v -timeout 30s
```

---

## 3.9 Manual cluster test

Terminal 1–3: start servers

Terminal 4:

```bash
# Find leader (try each until one works)
go run ./cmd/client -addr localhost:9001 -op SET -key name -value erfan
go run ./cmd/client -addr localhost:9002 -op GET -key name
go run ./cmd/client -addr localhost:9003 -op GET -key name
# All should return erfan
```

Kill leader, retry SET — should succeed after re-election.

---

## 3.10 Read handling (design choice)

**Simple (recommended for v1):** Only leader serves reads. Followers redirect.

**Stretch — linearizable reads:** Leader must verify it's still leader (AppendEntries quorum or ReadIndex) before serving GET. Implement after basic writes work.

Document your choice in README.

---

## 3.11 Common bugs (expect these)

| Bug | Symptom | Fix |
|-----|---------|-----|
| Off-by-one in log index | Replication always fails | Draw 1-indexed log on paper |
| Commit before majority | Data lost on crash | Check `advanceCommitIndex` |
| Applying uncommitted entries | Stale reads | Only apply up to `commitIndex` |
| Deadlock in `replicateOnce` | Hang under load | Never hold `mu` during network I/O |
| Not starting heartbeatLoop | Constant re-election | Call from `becomeLeader` |

---

## Checkpoint ✓

- [ ] SET replicates to all 3 nodes
- [ ] Killing leader, new leader accepts writes
- [ ] Divergent logs converge after conflict
- [ ] `go test -race ./internal/raft/` passes
- [ ] You can whiteboard the write path

---

## Next

→ **[Part 4: Persistence & Crash Recovery](04-persistence-and-safety.md)**

Nodes crash and restart. Without persistence, the log vanishes and safety breaks. You'll add a write-ahead log to disk.
