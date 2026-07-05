# Part 0: Getting Started

**Goal:** Understand what you're building, set up the project, and learn the Raft vocabulary before writing any consensus code.

**Time:** 1–2 hours

---

## 0.1 Why Raft exists (the problem in one paragraph)

Imagine three servers each storing a counter. A client sends "increment" to server A. Server A increments locally. But server B and C never hear about it — maybe the network dropped the message, maybe A crashed. Now the three servers disagree. **Which value is correct?**

You can't just pick "majority wins" naively — two servers might each think they're the majority during a network partition. That's **split brain**, and it causes data loss.

**Raft** solves this by electing exactly one **leader** at a time. All client writes go through the leader. The leader replicates each write to followers in a **log**. A write is **committed** only after a majority of nodes have stored it. If the leader dies, followers hold an election — but only nodes whose log is "up to date enough" can win.

You'll implement this yourself.

---

## 0.2 Raft vocabulary (memorize these)

| Term | Meaning |
|------|---------|
| **Node / Peer** | One server in the cluster (you'll run 3–5) |
| **Leader** | The only node that accepts client writes |
| **Follower** | Passive node; responds to leader/candidate RPCs |
| **Candidate** | A follower that is running an election |
| **Term** | Logical clock; increases on each election. Prevents stale leaders |
| **Log entry** | One command (e.g. `SET foo bar`) with a term number and index |
| **Commit index** | Highest log index known to be replicated on a majority |
| **State machine** | Your KV store — applies committed log entries in order |

### State diagram (draw this on paper)

```
         timeout / no leader          receives votes from majority
Follower ──────────────────────► Candidate ──────────────────────► Leader
   ▲                                  │                              │
   │         discovers higher term    │         discovers higher term│
   └──────────────────────────────────┴──────────────────────────────┘
```

Every node is always in exactly one of: **Follower**, **Candidate**, or **Leader**.

---

## 0.3 Architecture overview

Your system has four layers. Build bottom-up:

```
┌──────────────────────────────────────────────┐
│  Client (CLI) — GET / SET / DELETE           │
├──────────────────────────────────────────────┤
│  Raft layer — election, replication, commit  │
├──────────────────────────────────────────────┤
│  RPC / Transport — messages between nodes    │
├──────────────────────────────────────────────┤
│  State machine (KV store) + Persistence (WAL)│
└──────────────────────────────────────────────┘
```

**Part 1** builds the bottom two layers for a single node.  
**Parts 2–3** add the Raft layer.  
**Part 4** adds persistence.  
**Part 5** wraps transport in a testable network simulator.

---

## 0.4 Setup steps

### Step 1: Install Go

You need Go 1.21+. Check:

```bash
go version
```

If missing: https://go.dev/dl/

### Step 2: Initialize the module

From the project root:

```bash
cd raft-kv-store
go mod init github.com/YOUR_USERNAME/raft-kv-store
```

Replace `YOUR_USERNAME` with your GitHub username (or any module path you prefer).

### Step 3: Create the directory skeleton

```bash
mkdir -p cmd/server cmd/client
mkdir -p internal/kv internal/raft internal/rpc internal/persist internal/transport
touch BUGS.md
```

### Step 4: Create a minimal `cmd/server/main.go`

This is just a placeholder — you'll replace it in Part 1.

```go
package main

import "fmt"

func main() {
    fmt.Println("raft-kv-store: not implemented yet")
}
```

Verify it runs:

```bash
go run ./cmd/server
# Expected: raft-kv-store: not implemented yet
```

### Step 5: Install useful tools (optional but recommended)

```bash
# Static analysis (optional)
go install honnef.co/go/tools/cmd/staticcheck@latest
```

If `staticcheck` isn't found after install, add Go's bin directory to your PATH:

```bash
# Add to ~/.zshrc, then run: source ~/.zshrc
export PATH="$PATH:$(go env GOPATH)/bin"
```

**Race detector** — no separate install needed. It's built into Go. Use it when running tests (critical for Parts 2–3):

```bash
go test -race ./...
```

> **Note:** Older guides mention `loopclosure` as a separate tool. That package was removed from `golang.org/x/tools`; loop-closure bugs are caught by `go test -race` and modern Go compilers.

---

## 0.5 Read the Raft paper (strategically)

Don't read all 14 pages now. Read these sections and look at the figures:

| When | Read | Figure |
|------|------|--------|
| Part 2 | §5.2 Leader Election | Figure 2 (RequestVote RPC) |
| Part 3 | §5.3 Log Replication | Figures 3–6 |
| Part 4 | §5.4 Safety, §5.6 | Figure 8 (election restriction) |
| Part 4 | §5.7–5.8 | Persistence rules |

Bookmark: https://raft.github.io/raft.pdf

Also useful: the visual guide at https://thesecretlivesofdata.com/raft/

---

## 0.6 Design decisions (locked in for this guide)

These choices keep the project focused. You can change them later.

| Decision | Choice | Why |
|----------|--------|-----|
| Language | **Go** | Great concurrency, used by etcd/consul |
| Transport | **TCP + JSON** (not gRPC) | Fewer dependencies; you see the wire format |
| Cluster size | **3 nodes** default | Minimum for meaningful majority (2 of 3) |
| State machine | **In-memory map** | Simple; persistence is the Raft log, not the map |
| Client routing | **Any node** redirects to leader | Realistic; leader discovery is a learning goal |

---

## 0.7 Your `BUGS.md` template

Create `BUGS.md` now. You'll add entries as you go:

```markdown
# Bugs I Hit

## Template (copy for each bug)

### [Short title] — Part N
- **Symptom:** What you observed
- **Root cause:** Why it happened
- **How I found it:** Logging, test, diagram, etc.
- **Fix:** What you changed
- **Lesson:** What you'd tell an interviewer
```

This file becomes the "bugs I hit and how I found them" section recruiters love.

---

## Checkpoint ✓

Before moving to Part 1, confirm:

- [ ] `go mod init` succeeded
- [ ] Directory structure exists
- [ ] `go run ./cmd/server` prints the placeholder message
- [ ] You can explain: leader, follower, term, log entry, commit index
- [ ] You've sketched the Follower → Candidate → Leader state diagram
- [ ] `BUGS.md` exists

---

## Next

→ **[Part 1: Single-Node KV Store + RPC](01-single-node-kv.md)**

You will build a working key-value store that accepts GET/SET/DELETE over TCP — no Raft yet, but the foundation everything else sits on.
