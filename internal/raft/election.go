package raft

import (
	"math/rand"
	"time"

	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

func (n *Node) electionLoop() {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionReset:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomElectionTimeout())
		case <-timer.C:
			n.startElectionFromTimer()
			timer.Reset(randomElectionTimeout())
		}
	}
}

func randomElectionTimeout() time.Duration {
	delta := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(delta)))
}

func (n *Node) resetElectionTimer() {
	select {
	case n.electionReset <- struct{}{}:
	default:
	}
}

func (n *Node) lastLogInfo() (index, term int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) logIsUpToDate(candidateLastIndex, candidateLastTerm int) bool {
	lastIndex, lastTerm := n.lastLogInfo()
	if candidateLastTerm != lastTerm {
		return candidateLastTerm > lastTerm
	}
	return candidateLastIndex >= lastIndex
}

func (n *Node) becomeFollower(term int) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = noLeader
	n.knownLeader = noLeader
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.knownLeader = n.id
	lastIndex := len(n.log)
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}
	go n.heartbeatLoop()
}

func (n *Node) startElectionFromTimer() {
	n.mu.Lock()
	if n.state == Leader {
		n.mu.Unlock()
		return
	}

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastIdx, lastTerm := n.lastLogInfo()
	_ = n.persistState()

	args := rpc.RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	votes := 1
	majority := n.majority()
	peers := append([]int(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		reply, err := n.transport.RequestVote(peer, args)
		if err != nil {
			continue
		}
		n.mu.Lock()
		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term)
			_ = n.persistState()
			n.mu.Unlock()
			return
		}
		if reply.VoteGranted && n.currentTerm == term {
			votes++
		}
		n.mu.Unlock()
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if votes >= majority && n.state == Candidate && n.currentTerm == term {
		n.becomeLeader()
	}
}
