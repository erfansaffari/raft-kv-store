# Raft KV Store — Build-It-Yourself Guide

A distributed key-value store (`GET`, `SET`, `DELETE`) backed by the **Raft consensus algorithm**, built from scratch in Go.

> **How to use this repo:** Read the docs in order. Each part has theory, steps, code to write, checkpoints, and exercises. **You write the code** — these files teach and guide, they are not a copy-paste solution.

## What you'll build

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node 1  │◄───►│ Node 2  │◄───►│ Node 3  │
│ (Leader)│     │(Follower)│    │(Follower)│
└────┬────┘     └─────────┘     └─────────┘
     │
  Client CLI
  GET / SET / DELETE
```

- 3–5 nodes that elect a leader automatically
- Writes replicated to a majority before commit
- Survives leader crashes and network partitions
- Persists state to disk and recovers after restart
- Chaos-tested for linearizability

## Prerequisites

| Skill | Level needed |
|-------|-------------|
| Go basics (structs, interfaces, goroutines, channels) | Comfortable |
| TCP/sockets or HTTP | Basic |
| Concurrency (mutexes, race conditions) | Will learn as you go |
| Distributed systems | None — you'll learn Raft here |

**Recommended reading before Part 1:** Skim the [Raft paper figures 2–8](https://raft.github.io/raft.pdf) (don't memorize — you'll revisit each figure when relevant).

## Learning path

| Part | File | What you'll implement | Time estimate |
|------|------|----------------------|---------------|
| 0 | [docs/00-getting-started.md](docs/00-getting-started.md) | Project setup, architecture overview | 1–2 hours |
| 1 | [docs/01-single-node-kv.md](docs/01-single-node-kv.md) | KV store + RPC layer (no consensus) | 1 weekend |
| 2 | [docs/02-leader-election.md](docs/02-leader-election.md) | Raft leader election | 1 weekend |
| 3 | [docs/03-log-replication.md](docs/03-log-replication.md) | Log replication + client writes | 1–2 weekends |
| 4 | [docs/04-persistence-and-safety.md](docs/04-persistence-and-safety.md) | WAL persistence + crash recovery | 1 weekend |
| 5 | [docs/05-chaos-testing.md](docs/05-chaos-testing.md) | Network simulator + linearizability tests | 1–2 weekends |
| 6 | [docs/06-polish-and-writeup.md](docs/06-polish-and-writeup.md) | CLI client, README, interview prep | Ongoing |

**Appendix:** [docs/appendix-raft-reference.md](docs/appendix-raft-reference.md) — quick reference while coding

## Project layout (you'll create this over time)

```
raft-kv-store/
├── cmd/
│   ├── server/main.go          # Start a Raft node
│   └── client/main.go          # CLI client (Part 6)
├── internal/
│   ├── kv/                     # Key-value state machine
│   ├── raft/                   # Raft core (election, replication)
│   ├── rpc/                    # Wire protocol between nodes
│   ├── persist/                # Write-ahead log (Part 4)
│   └── transport/              # Network layer + simulator (Part 5)
├── docs/                       # These guides
├── go.mod
└── README.md
```

## Rules for learning (read this)

1. **Type the code yourself.** Copy-pasting skips the learning.
2. **Run checkpoints after every section.** If something fails, fix it before moving on.
3. **Draw diagrams.** Raft makes sense when you sketch state transitions on paper.
4. **Log everything during Parts 2–3.** You'll need logs to debug split votes and log divergence.
5. **Keep a `BUGS.md` file.** Note every bug you hit and how you found it — this becomes your interview gold.

## How to get help from the AI

When you're stuck, ask with context:

```
"I'm on Part 2, Step 3. My RequestVote RPC returns false every time.
Here's my Node struct and startElection function: [paste code]
Here's the log output: [paste logs]"
```

Good questions beat "fix my code."

## Start here

→ Open **[docs/00-getting-started.md](docs/00-getting-started.md)** and begin Part 0.
