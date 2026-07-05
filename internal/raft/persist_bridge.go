package raft

import (
	"github.com/erfansaffari/raft-kv-store/internal/persist"
	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

func (n *Node) persistState() error {
	if n.persist == nil {
		return nil
	}
	return n.persist.Save(persist.State{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         append([]rpc.LogEntry(nil), n.log...),
		CommitIndex: n.commitIndex,
	})
}
