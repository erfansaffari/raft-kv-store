package raft

import (
	"time"

	"github.com/erfansaffari/raft-kv-store/internal/command"
	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			n.replicateToAll()
			n.mu.Unlock()
		}
	}
}

func (n *Node) replicateToAll() {
	for _, peer := range n.peers {
		go n.replicateOnce(peer)
	}
}

func (n *Node) replicateOnce(peer int) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex > 0 && prevLogIndex <= len(n.log) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	var entries []rpc.LogEntry
	if nextIdx-1 < len(n.log) {
		entries = append([]rpc.LogEntry(nil), n.log[nextIdx-1:]...)
	}
	args := rpc.AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	n.mu.Unlock()

	reply, err := n.transport.AppendEntries(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		_ = n.persistState()
		return
	}
	if n.state != Leader || term != n.currentTerm {
		return
	}

	if reply.Success {
		if len(entries) > 0 {
			lastNew := entries[len(entries)-1].Index
			n.matchIndex[peer] = lastNew
			n.nextIndex[peer] = lastNew + 1
		} else if args.PrevLogIndex > 0 {
			n.matchIndex[peer] = max(n.matchIndex[peer], args.PrevLogIndex)
		}
		n.advanceCommitIndex()
		return
	}

	if reply.ConflictIndex > 0 {
		n.nextIndex[peer] = reply.ConflictIndex
	} else {
		n.nextIndex[peer] = max(1, n.nextIndex[peer]-1)
	}
}

func (n *Node) advanceCommitIndex() {
	for idx := len(n.log); idx > n.commitIndex; idx-- {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}
		count := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			break
		}
	}
}

type NotLeaderError struct {
	LeaderID int
}

func (e *NotLeaderError) Error() string {
	return "not leader"
}

func (n *Node) SubmitCommand(cmd command.Command) (command.ApplyResult, error) {
	n.mu.Lock()
	if n.state != Leader {
		leader := n.knownLeader
		n.mu.Unlock()
		return command.ApplyResult{}, &NotLeaderError{LeaderID: leader}
	}

	index := len(n.log) + 1
	entry := rpc.LogEntry{
		Term:    n.currentTerm,
		Index:   index,
		Command: cmd,
	}
	waitCh := make(chan command.ApplyResult, 1)
	n.applyWaiters[index] = waitCh
	n.log = append(n.log, entry)
	_ = n.persistState()
	n.mu.Unlock()

	n.replicateToAll()

	select {
	case result := <-waitCh:
		return result, nil
	case <-time.After(2 * time.Second):
		return command.ApplyResult{}, &NotLeaderError{LeaderID: n.knownLeader}
	}
}

func (n *Node) Get(key string) (command.ApplyResult, error) {
	n.mu.Lock()
	isLeader := n.state == Leader
	leader := n.knownLeader
	n.mu.Unlock()

	if !isLeader {
		return command.ApplyResult{}, &NotLeaderError{LeaderID: leader}
	}
	return n.store.Apply(command.Command{Op: "GET", Key: key}), nil
}
