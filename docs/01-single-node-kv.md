# Part 1: Single-Node KV Store + RPC Layer

**Goal:** A working key-value store on one node with a TCP RPC layer. No consensus yet — this is your state machine and wire protocol.

**Time:** ~1 weekend

**You'll learn:** State machine design, RPC request/response patterns, applying commands from a log (even though the log is trivial for now).

---

## 1.1 What you're building in this part

```
Client                    Server (single node)
  │                              │
  │  TCP: {"op":"SET","key":"a","value":"1"}
  ├─────────────────────────────►│
  │                              │  apply to KV map
  │  {"ok":true}                 │
  │◄─────────────────────────────┤
```

Later, Raft will sit **between** the RPC handler and the KV store. The KV store interface you build now should not change much.

---

## 1.2 Step 1 — Define the command types

Create `internal/kv/command.go`:

```go
package kv

// Command is one operation in the replicated log.
// Raft doesn't interpret these — it just replicates them.
// The state machine (KV store) applies them in log order.
type Command struct {
    Op    string `json:"op"`              // "GET", "SET", "DELETE"
    Key   string `json:"key,omitempty"`
    Value string `json:"value,omitempty"`
}

// ApplyResult is returned when a command is applied to the state machine.
type ApplyResult struct {
    Value string `json:"value,omitempty"` // for GET
    Found bool   `json:"found,omitempty"` // for GET / DELETE
}
```

**Why a log of commands, not direct map access?** Raft replicates a **log of commands**. Even on a single node, treating writes as "append command, then apply" matches the Raft model.

---

## 1.3 Step 2 — Implement the state machine

Create `internal/kv/store.go`:

```go
package kv

import "sync"

// Store is the key-value state machine.
// It must be safe for concurrent access (Raft will call Apply from one goroutine,
// but reads may happen elsewhere during testing).
type Store struct {
    mu   sync.RWMutex
    data map[string]string
}

func NewStore() *Store {
    return &Store{data: make(map[string]string)}
}

// Apply executes one command and returns the result.
// This is the ONLY way the map should mutate.
func (s *Store) Apply(cmd Command) ApplyResult {
    s.mu.Lock()
    defer s.mu.Unlock()

    switch cmd.Op {
    case "SET":
        s.data[cmd.Key] = cmd.Value
        return ApplyResult{}
    case "DELETE":
        _, found := s.data[cmd.Key]
        delete(s.data, cmd.Key)
        return ApplyResult{Found: found}
    case "GET":
        val, found := s.data[cmd.Key]
        return ApplyResult{Value: val, Found: found}
    default:
        return ApplyResult{}
    }
}

// Snapshot returns a copy of the map (useful for tests and later snapshotting).
func (s *Store) Snapshot() map[string]string {
    s.mu.RLock()
    defer s.mu.RUnlock()
    copy := make(map[string]string, len(s.data))
    for k, v := range s.data {
        copy[k] = v
    }
    return copy
}
```

### Exercise: Write tests first

Create `internal/kv/store_test.go`. Write tests for SET, GET (found/not found), DELETE, and applying commands in sequence. Run:

```bash
go test ./internal/kv/ -v
```

**Checkpoint:** All KV tests pass.

---

## 1.4 Step 3 — Define the RPC wire format

Create `internal/rpc/types.go`:

```go
package rpc

import "github.com/YOUR_USERNAME/raft-kv-store/internal/kv"

// ClientRequest is sent by clients to any node.
type ClientRequest struct {
    ID    string     `json:"id"`    // unique request ID (for dedup later)
    Cmd   kv.Command `json:"cmd"`
}

// ClientResponse is returned to clients.
type ClientResponse struct {
    OK     bool            `json:"ok"`
    Result kv.ApplyResult  `json:"result,omitempty"`
    Error  string          `json:"error,omitempty"`

    // Filled in once Raft exists:
    LeaderID string `json:"leader_id,omitempty"` // redirect hint
}

// --- Internal Raft RPCs (stubbed for now, used in Part 2+) ---

type RequestVoteArgs struct {
    Term         int `json:"term"`
    CandidateID  int `json:"candidate_id"`
    LastLogIndex int `json:"last_log_index"`
    LastLogTerm  int `json:"last_log_term"`
}

type RequestVoteReply struct {
    Term        int  `json:"term"`
    VoteGranted bool `json:"vote_granted"`
}

type AppendEntriesArgs struct {
    Term         int         `json:"term"`
    LeaderID     int         `json:"leader_id"`
    PrevLogIndex int         `json:"prev_log_index"`
    PrevLogTerm  int         `json:"prev_log_term"`
    Entries      []LogEntry  `json:"entries"`
    LeaderCommit int         `json:"leader_commit"`
}

type AppendEntriesReply struct {
    Term          int  `json:"term"`
    Success       bool `json:"success"`
    ConflictIndex int  `json:"conflict_index,omitempty"`
    ConflictTerm  int  `json:"conflict_term,omitempty"`
}

// LogEntry is one slot in the Raft log.
type LogEntry struct {
    Term    int        `json:"term"`
    Index   int        `json:"index"`
    Command kv.Command `json:"command"`
}
```

Replace `YOUR_USERNAME` with your module path.

---

## 1.5 Step 4 — JSON-over-TCP codec

Create `internal/rpc/codec.go`:

```go
package rpc

import (
    "encoding/json"
    "io"
)

// Message wraps any RPC payload with a type tag.
type Message struct {
    Type string          `json:"type"`
    Body json.RawMessage `json:"body"`
}

const (
    MsgClientRequest  = "client_request"
    MsgClientResponse = "client_response"
    // Raft types added in Part 2:
    MsgRequestVote    = "request_vote"
    MsgRequestVoteReply = "request_vote_reply"
    MsgAppendEntries  = "append_entries"
    MsgAppendEntriesReply = "append_entries_reply"
)

func WriteMessage(w io.Writer, msgType string, body any) error {
    raw, err := json.Marshal(body)
    if err != nil {
        return err
    }
    msg := Message{Type: msgType, Body: raw}
    data, err := json.Marshal(msg)
    if err != nil {
        return err
    }
    // Length-prefix framing: 4-byte big-endian length + JSON
    header := []byte{
        byte(len(data) >> 24),
        byte(len(data) >> 16),
        byte(len(data) >> 8),
        byte(len(data)),
    }
    if _, err := w.Write(append(header, data...)); err != nil {
        return err
    }
    return nil
}

func ReadMessage(r io.Reader) (Message, error) {
    header := make([]byte, 4)
    if _, err := io.ReadFull(r, header); err != nil {
        return Message{}, err
    }
    length := int(header[0])<<24 | int(header[1])<<16 | int(header[2])<<8 | int(header[3])
    data := make([]byte, length)
    if _, err := io.ReadFull(r, data); err != nil {
        return Message{}, err
    }
    var msg Message
    if err := json.Unmarshal(data, &msg); err != nil {
        return Message{}, err
    }
    return msg, nil
}
```

### Why length-prefix framing?

TCP is a stream — one `Write` doesn't map to one `Read`. Prefixing each message with its length lets you parse messages reliably.

### Exercise: Test the codec

Create `internal/rpc/codec_test.go` — write a message to a buffer, read it back, assert equality.

---

## 1.6 Step 5 — Single-node server

Create `internal/kv/server.go` — handles one client connection on a single node (no Raft):

```go
package kv

import (
    "encoding/json"
    "net"

    "github.com/YOUR_USERNAME/raft-kv-store/internal/rpc"
)

// Server is a single-node KV server (Part 1 only).
type Server struct {
    store *Store
    ln    net.Listener
}

func NewServer(addr string) (*Server, error) {
    ln, err := net.Listen("tcp", addr)
    if err != nil {
        return nil, err
    }
    return &Server{store: NewStore(), ln: ln}, nil
}

func (s *Server) Serve() error {
    for {
        conn, err := s.ln.Accept()
        if err != nil {
            return err
        }
        go s.handleConn(conn)
    }
}

func (s *Server) handleConn(conn net.Conn) {
    defer conn.Close()
    for {
        msg, err := rpc.ReadMessage(conn)
        if err != nil {
            return
        }
        switch msg.Type {
        case rpc.MsgClientRequest:
            var req rpc.ClientRequest
            if err := json.Unmarshal(msg.Body, &req); err != nil {
                return
            }
            result := s.store.Apply(req.Cmd)
            _ = rpc.WriteMessage(conn, rpc.MsgClientResponse, rpc.ClientResponse{
                OK:     true,
                Result: result,
            })
        }
    }
}

func (s *Server) Addr() string {
    return s.ln.Addr().String()
}

func (s *Server) Close() error {
    return s.ln.Close()
}
```

Update `cmd/server/main.go`:

```go
package main

import (
    "flag"
    "log"

    "github.com/YOUR_USERNAME/raft-kv-store/internal/kv"
)

func main() {
    addr := flag.String("addr", ":9001", "listen address")
    flag.Parse()

    srv, err := kv.NewServer(*addr)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("single-node KV server listening on %s", srv.Addr())
    log.Fatal(srv.Serve())
}
```

---

## 1.7 Step 6 — Minimal CLI client

Create `cmd/client/main.go`:

```go
package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net"

    "github.com/YOUR_USERNAME/raft-kv-store/internal/kv"
    "github.com/YOUR_USERNAME/raft-kv-store/internal/rpc"
)

func main() {
    addr := flag.String("addr", "localhost:9001", "server address")
    op := flag.String("op", "GET", "GET, SET, or DELETE")
    key := flag.String("key", "", "key")
    value := flag.String("value", "", "value (for SET)")
    flag.Parse()

    conn, err := net.Dial("tcp", *addr)
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    req := rpc.ClientRequest{
        ID: fmt.Sprintf("req-%d", time.Now().UnixNano()),
        Cmd: kv.Command{Op: *op, Key: *key, Value: *value},
    }
    if err := rpc.WriteMessage(conn, rpc.MsgClientRequest, req); err != nil {
        log.Fatal(err)
    }

    msg, err := rpc.ReadMessage(conn)
    if err != nil {
        log.Fatal(err)
    }
    var resp rpc.ClientResponse
    if err := json.Unmarshal(msg.Body, &resp); err != nil {
        log.Fatal(err)
    }
    if !resp.OK {
        log.Fatalf("error: %s", resp.Error)
    }
    fmt.Printf("%+v\n", resp.Result)
}
```

Add `"time"` to imports. Fix any compile errors.

---

## 1.8 Manual test script

Terminal 1:

```bash
go run ./cmd/server -addr :9001
```

Terminal 2:

```bash
go run ./cmd/client -op SET -key foo -value bar
go run ./cmd/client -op GET -key foo
# Expected: {Value:bar Found:true}

go run ./cmd/client -op DELETE -key foo
go run ./cmd/client -op GET -key foo
# Expected: {Value: Found:false}
```

---

## 1.9 Step 7 — Introduce a local "log" (prep for Raft)

Even on one node, append commands to an in-memory log before applying. This mirrors Raft's flow.

Create `internal/kv/log.go`:

```go
package kv

type Log struct {
    entries []Command
}

func NewLog() *Log {
    return &Log{entries: make([]Command, 0)}
}

func (l *Log) Append(cmd Command) int {
    l.entries = append(l.entries, cmd)
    return len(l.entries) // 1-based index
}

func (l *Log) Get(index int) (Command, bool) {
    if index < 1 || index > len(l.entries) {
        return Command{}, false
    }
    return l.entries[index-1], true
}

func (l *Log) Len() int {
    return len(l.entries)
}
```

Refactor `Server` to:

1. Append write commands (`SET`, `DELETE`) to the log
2. Apply to the store
3. For `GET`, read directly from store (Raft handles reads differently later — linearizable vs stale reads is a Part 3 topic)

**Checkpoint:** Server still passes manual tests after refactor.

---

## 1.10 Concepts to understand before Part 2

Answer these in your own words (write in `BUGS.md` or a notes file):

1. Why does Raft replicate a **log** instead of replicating the map directly?
2. What does "deterministic state machine" mean? (Same commands in same order → same state)
3. Why length-prefix framing on TCP?
4. What will change when Raft sits in front of `handleConn`?

---

## Common mistakes in Part 1

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Forgetting `go` on `handleConn` | Server handles one client only | `go s.handleConn(conn)` |
| No mutex on map | Race detector screams in tests | Use `sync.RWMutex` |
| Reading TCP without length prefix | JSON parse errors, hangs | Use the codec consistently |
| Mutating map outside `Apply` | Breaks later Raft assumptions | All mutations through `Apply` |

---

## Checkpoint ✓

- [ ] `go test ./internal/kv/ ./internal/rpc/ -v` passes
- [ ] Server starts and client SET/GET/DELETE works
- [ ] Commands append to local log before apply
- [ ] You can explain why the state machine is separate from Raft

---

## Next

→ **[Part 2: Leader Election](02-leader-election.md)**

You'll add the Raft node struct, randomized election timeouts, and `RequestVote` RPCs. No log replication yet — just electing a leader.
