package rpc

import "github.com/erfansaffari/raft-kv-store/internal/kv"

// ClientRequest is sent by clients to any node.
type ClientRequest struct {
	ID  string     `json:"id"` // unique request ID (for dedup later)
	Cmd kv.Command `json:"cmd"`
}

// ClientResponse is returned to clients.
type ClientResponse struct {
	OK     bool           `json:"ok"`
	Result kv.ApplyResult `json:"result,omitempty"`
	Error  string         `json:"error,omitempty"`

	// Filled in once Raft exists:
	LeaderID string `json:"leader_id,omitempty"` // redirect hint
}

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
	Term         int        `json:"term"`
	LeaderID     int        `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
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
