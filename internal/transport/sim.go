package transport

import (
	"sync"
	"time"

	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

type SimNetwork struct {
	mu         sync.Mutex
	inner      *MemoryNetwork
	partitions [][]int
	disabled   map[int]bool
}

func NewSimNetwork() *SimNetwork {
	return &SimNetwork{
		inner:    NewMemoryNetwork(),
		disabled: make(map[int]bool),
	}
}

func (s *SimNetwork) Register(id int, node rpc.Receiver) {
	s.inner.Register(id, node)
}

func (s *SimNetwork) Unregister(id int) {
	s.inner.Unregister(id)
}

func (s *SimNetwork) Kill(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disabled[id] = true
}

func (s *SimNetwork) Revive(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.disabled, id)
}

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
			if id == from {
				fromGroup = i
			}
			if id == to {
				toGroup = i
			}
		}
	}
	return fromGroup == toGroup && fromGroup >= 0
}

type simTransport struct {
	net    *SimNetwork
	nodeID int
}

func NewSimTransport(net *SimNetwork, nodeID int) *simTransport {
	return &simTransport{net: net, nodeID: nodeID}
}

func (t *simTransport) RequestVote(targetID int, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error) {
	t.net.mu.Lock()
	ok := t.net.canDeliver(t.nodeID, targetID)
	var target rpc.Receiver
	if ok {
		target = t.net.inner.nodes[targetID]
	}
	t.net.mu.Unlock()
	if !ok || target == nil {
		return rpc.RequestVoteReply{}, nil
	}
	return target.HandleRequestVote(args), nil
}

func (t *simTransport) AppendEntries(targetID int, args rpc.AppendEntriesArgs) (rpc.AppendEntriesReply, error) {
	t.net.mu.Lock()
	ok := t.net.canDeliver(t.nodeID, targetID)
	var target rpc.Receiver
	if ok {
		target = t.net.inner.nodes[targetID]
	}
	t.net.mu.Unlock()
	if !ok || target == nil {
		return rpc.AppendEntriesReply{}, nil
	}
	time.Sleep(1 * time.Millisecond)
	return target.HandleAppendEntries(args), nil
}
