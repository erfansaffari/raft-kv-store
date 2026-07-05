package raft

import (
	"sync"
	"time"

	"github.com/erfansaffari/raft-kv-store/internal/command"
	"github.com/erfansaffari/raft-kv-store/internal/kv"
	"github.com/erfansaffari/raft-kv-store/internal/persist"
	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	noLeader           = -1
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

type Transport interface {
	RequestVote(targetID int, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error)
	AppendEntries(targetID int, args rpc.AppendEntriesArgs) (rpc.AppendEntriesReply, error)
}

type Node struct {
	mu sync.Mutex

	id    int
	peers []int

	state State

	currentTerm int
	votedFor    int
	log         []rpc.LogEntry

	commitIndex int
	lastApplied int

	nextIndex  map[int]int
	matchIndex map[int]int

	electionReset chan struct{}
	stopCh        chan struct{}

	transport Transport
	persist   persist.Store
	store     *kv.Store

	applyWaiters map[int]chan command.ApplyResult
	knownLeader  int
}

func NewNode(id int, peers []int, store *kv.Store, transport Transport, p persist.Store) (*Node, error) {
	if p == nil {
		p = persist.NewMemoryStore()
	}
	state, err := p.Load()
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:            id,
		peers:         peers,
		state:         Follower,
		currentTerm:   state.CurrentTerm,
		votedFor:      state.VotedFor,
		log:           state.Log,
		commitIndex:   state.CommitIndex,
		nextIndex:     make(map[int]int),
		matchIndex:    make(map[int]int),
		electionReset: make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		transport:     transport,
		persist:       p,
		store:         store,
		applyWaiters:  make(map[int]chan command.ApplyResult),
		knownLeader:   noLeader,
	}
	if n.votedFor == 0 {
		n.votedFor = noLeader
	}
	if n.log == nil {
		n.log = make([]rpc.LogEntry, 0)
	}
	n.replayCommitted()
	return n, nil
}

func (n *Node) Run() {
	go n.electionLoop()
	go n.applyLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) ID() int { return n.id }

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) CurrentTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) KnownLeader() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.knownLeader
}

func (n *Node) Store() *kv.Store { return n.store }

func (n *Node) peersCopy() []int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]int(nil), n.peers...)
}

func (n *Node) LogLen() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.log)
}

func (n *Node) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func stateString(s State) string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

func (n *Node) clusterSize() int {
	return len(n.peers) + 1
}

func (n *Node) majority() int {
	return n.clusterSize()/2 + 1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
