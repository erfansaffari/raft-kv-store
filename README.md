# raft-kv-store

Distributed fault-tolerant key-value store using the **Raft consensus algorithm**, built from scratch in Go.

## Features

- Leader election with randomized timeouts
- Log replication with majority commit
- Persistent state (optional `-data` flag)
- Network partition chaos tests
- 3-node cluster with client redirect to leader

## Architecture

```
Client ──► Node (client port :900N)
              │
              ├── Raft core (election, replication, commit)
              ├── KV state machine (GET/SET/DELETE)
              └── Persistent log (optional WAL JSON file)

Node ◄──Raft RPC──► Node  (raft port :910N)
```

## Quick start

### Run tests (start here to verify everything works)

```bash
go test -race ./...
```

### Run a 3-node cluster

Terminal 1:

```bash
./scripts/run-cluster.sh
```

Terminal 2:

```bash
go run ./cmd/client -op SET -key foo -value bar
go run ./cmd/client -op GET -key foo
```

Or interactive REPL:

```bash
go run ./cmd/client -repl
```

## Learning guides

Step-by-step build guides live in `docs/`:

| Part | Topic |
|------|-------|
| 0 | Setup & Raft vocabulary |
| 1 | Single-node KV + RPC |
| 2 | Leader election |
| 3 | Log replication |
| 4 | Persistence |
| 5 | Chaos testing |
| 6 | Polish & interview prep |

## Manual cluster (without script)

```bash
go run ./cmd/server -id 1 -peers 1,2,3 -addr :9001 -raft-addr :9101 -data ./data/node-1
go run ./cmd/server -id 2 -peers 1,2,3 -addr :9002 -raft-addr :9102 -data ./data/node-2
go run ./cmd/server -id 3 -peers 1,2,3 -addr :9003 -raft-addr :9103 -data ./data/node-3
```

## Project layout

```
cmd/server/          # Raft node process
cmd/client/          # CLI client
internal/command/    # Shared command types
internal/kv/         # Key-value state machine
internal/raft/       # Raft consensus core
internal/rpc/        # Wire protocol
internal/persist/    # Disk persistence
internal/transport/  # Memory, sim, and TCP transports
internal/cluster/    # Wires raft + network + client server
docs/                # Learning guides
scripts/             # Cluster launcher
```

## Bugs I hit

See [BUGS.md](BUGS.md).
