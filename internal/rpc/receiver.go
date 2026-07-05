package rpc

// Receiver is implemented by Raft nodes for inter-node RPC.
type Receiver interface {
	HandleRequestVote(args RequestVoteArgs) RequestVoteReply
	HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
}
