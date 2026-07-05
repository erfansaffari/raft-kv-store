# Testing Guide — What to Run After Each Part

Use this checklist to verify each milestone and understand what changed.

---

## Part 1 — Single-node KV + RPC

**What was built:** `internal/kv/`, `internal/rpc/`, `internal/command/`, single-node server/client.

```bash
go test ./internal/kv/ ./internal/rpc/ -v
go run ./cmd/server -addr :9001          # if using single-node mode
go run ./cmd/client -addr localhost:9001 -op SET -key foo -value bar
go run ./cmd/client -addr localhost:9001 -op GET -key foo
```

**What to understand:** Commands go through a state machine; RPC uses length-prefixed JSON; `SET` returns empty result, `GET` returns value.

---

## Part 2 — Leader election

**What was built:** `internal/raft/` election logic, `internal/transport/memory.go`.

```bash
go test ./internal/raft/ -run TestElect -race -v
```

**What to understand:** 3 nodes elect exactly one leader; killing the leader triggers re-election within ~1s.

---

## Part 3 — Log replication

**What was built:** AppendEntries, commit index, apply loop, `internal/cluster/`.

```bash
go test ./internal/raft/ -run 'TestBasic|TestWrite|TestNotLeader' -race -v
./scripts/run-cluster.sh   # terminal 1
go run ./cmd/client -op SET -key x -value 1   # terminal 2
go run ./cmd/client -op GET -key x
```

**What to understand:** Writes go through the leader only; followers redirect; data replicates to all nodes.

---

## Part 4 — Persistence

**What was built:** `internal/persist/`, `-data` flag on server.

```bash
go test ./internal/raft/ -run TestPersist -race -v
# Start cluster with -data ./data/node-N, write data, kill all, restart — data survives
```

**What to understand:** Term, votedFor, log, and commitIndex survive restart via JSON file.

---

## Part 5 — Chaos testing

**What was built:** `internal/transport/sim.go`, partition tests.

```bash
go test ./internal/raft/ -run TestChaos -race -v
go test -race ./... -count=3
```

**What to understand:** Majority partition can write; minority cannot; after heal all nodes converge.

---

## Part 6 — Polish

**What was built:** REPL client, cluster script, README.

```bash
go run ./cmd/client -repl
./scripts/run-cluster.sh
```

---

## Full test suite (run anytime)

```bash
go test -race ./...
```
