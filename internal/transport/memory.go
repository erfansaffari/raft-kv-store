package transport

import (
	"sync"

	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

type MemoryNetwork struct {
	mu    sync.Mutex
	nodes map[int]rpc.Receiver
}

func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{nodes: make(map[int]rpc.Receiver)}
}

func (m *MemoryNetwork) Register(id int, node rpc.Receiver) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[id] = node
}

func (m *MemoryNetwork) Unregister(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, id)
}

type memoryTransport struct {
	net    *MemoryNetwork
	nodeID int
}

func NewMemoryTransport(net *MemoryNetwork, nodeID int) *memoryTransport {
	return &memoryTransport{net: net, nodeID: nodeID}
}

func (t *memoryTransport) RequestVote(targetID int, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error) {
	t.net.mu.Lock()
	target, ok := t.net.nodes[targetID]
	t.net.mu.Unlock()
	if !ok {
		return rpc.RequestVoteReply{}, nil
	}
	return target.HandleRequestVote(args), nil
}

func (t *memoryTransport) AppendEntries(targetID int, args rpc.AppendEntriesArgs) (rpc.AppendEntriesReply, error) {
	t.net.mu.Lock()
	target, ok := t.net.nodes[targetID]
	t.net.mu.Unlock()
	if !ok {
		return rpc.AppendEntriesReply{}, nil
	}
	return target.HandleAppendEntries(args), nil
}
