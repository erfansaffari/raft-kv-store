# Raft Quick Reference

Keep this open while coding. Numbers refer to the [Raft paper](https://raft.github.io/raft.pdf).

---

## States

| State | Behavior |
|-------|----------|
| Follower | Passive; responds to RPCs; starts election on timeout |
| Candidate | Request votes; becomes leader if majority grants |
| Leader | Accept client writes; replicate log; send heartbeats |

---

## Persistent vs volatile

**Persist (disk before reply):**
- `currentTerm`
- `votedFor`
- `log[]`
- (practical add) `commitIndex`

**Volatile (all nodes):**
- `commitIndex`
- `lastApplied`

**Volatile (leader only):**
- `nextIndex[peer]` — next log entry to send
- `matchIndex[peer]` — highest known replicated on peer

---

## RequestVote RPC

**Args:** `term`, `candidateId`, `lastLogIndex`, `lastLogTerm`

**Reply:** `term`, `voteGranted`

**Receiver rules:**
1. `false` if `term < currentTerm`
2. If `term > currentTerm` → update term, become follower
3. Grant if haven't voted this term AND candidate log is ≥ up-to-date
4. Reset election timer on valid RPC from leader/candidate

**Up-to-date:** Compare last log term first, then index.

---

## AppendEntries RPC

**Args:** `term`, `leaderId`, `prevLogIndex`, `prevLogTerm`, `entries[]`, `leaderCommit`

**Reply:** `term`, `success`, (`conflictIndex`, `conflictTerm` on failure)

**Receiver rules:**
1. `false` if `term < currentTerm`
2. Reset election timer
3. If log doesn't contain entry at `prevLogIndex` with `prevLogTerm` → `false` + conflict hints
4. Delete conflicting entries; append new ones
5. If `leaderCommit > commitIndex` → `commitIndex = min(leaderCommit, lastLogIndex)`

**Leader on success:** update `matchIndex`, `nextIndex`, maybe advance `commitIndex`

**Leader on failure:** decrement `nextIndex` (or jump to conflictIndex)

---

## Commit rules (Figure 8)

- Entry committed when **replicated on majority**
- Leader only commits entries from **current term** by counting replicas (indirect commit of older terms OK once current-term entry commits)

---

## Timing (paper guidance)

```
broadcastTime << electionTimeout << MTBF
```

Typical:
- Heartbeat: 50ms
- Election timeout: 150–300ms (randomized)
- MTBF: hours (hardware)

---

## Invariants (assert in debug)

```go
commitIndex <= len(log)
lastApplied <= commitIndex
// log indices are 1..len, contiguous
// terms in log are non-decreasing? NO — terms increase on re-election at same index
```

---

## Log index cheat sheet

```
Index:  1    2    3    4
Term:   1    1    2    2
Cmd:   SET  SET  SET  DEL
        a    b    c    c
```

- `len(log)` = 4
- `lastLogIndex` = 4
- `lastLogTerm` = 2
- New entry index = 5

Arrays in Go are 0-indexed → `log[i-1]` is entry at index `i`.

---

## Common interview one-liners

| Question | Answer |
|----------|--------|
| Why Raft over Paxos? | Understandable; strong leader; decomposed |
| Split vote? | Randomized election timeouts |
| Stale leader writes? | Step down if `term` in reply > own term |
| Why log not state? | Deterministic replay; ordering; compact |
| Minority partition? | Can't commit; can't elect leader |
| Exactly-once client ops? | Idempotent client IDs + dedup on leader |

---

## Useful links

- Paper: https://raft.github.io/raft.pdf
- Visual: https://thesecretlivesofdata.com/raft/
- etcd Raft: https://github.com/etcd-io/raft (read after yours works — not before)
- Jepsen: https://jepsen.io/ (what "proven correct" looks like in industry)
