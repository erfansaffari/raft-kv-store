package raft

import (
	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

func (n *Node) HandleRequestVote(args rpc.RequestVoteArgs) rpc.RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := rpc.RequestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
		_ = n.persistState()
	}

	if (n.votedFor == noLeader || n.votedFor == args.CandidateID) &&
		n.logIsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimer()
		_ = n.persistState()
	}

	return reply
}

func (n *Node) HandleAppendEntries(args rpc.AppendEntriesArgs) rpc.AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := rpc.AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(args.Term)
		_ = n.persistState()
	}

	n.knownLeader = args.LeaderID
	n.resetElectionTimer()

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(n.log) {
			reply.ConflictIndex = len(n.log) + 1
			return reply
		}
		prev := n.log[args.PrevLogIndex-1]
		if prev.Term != args.PrevLogTerm {
			reply.ConflictTerm = prev.Term
			for i := args.PrevLogIndex; i >= 1; i-- {
				if n.log[i-1].Term != prev.Term {
					reply.ConflictIndex = i + 1
					break
				}
				if i == 1 {
					reply.ConflictIndex = 1
				}
			}
			return reply
		}
	}

	insertAt := args.PrevLogIndex
	for i, newEntry := range args.Entries {
		idx := insertAt + i + 1
		if idx <= len(n.log) {
			if n.log[idx-1].Term != newEntry.Term {
				n.log = n.log[:idx-1]
			}
		}
		if idx > len(n.log) {
			n.log = append(n.log, newEntry)
		}
	}

	if len(args.Entries) > 0 {
		_ = n.persistState()
	}

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, len(n.log))
	}

	reply.Success = true
	return reply
}
