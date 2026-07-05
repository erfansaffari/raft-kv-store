package persist

import "github.com/erfansaffari/raft-kv-store/internal/rpc"

type State struct {
	CurrentTerm int            `json:"current_term"`
	VotedFor    int            `json:"voted_for"`
	Log         []rpc.LogEntry `json:"log"`
	CommitIndex int            `json:"commit_index"`
}

type Store interface {
	Save(state State) error
	Load() (State, error)
}

type MemoryStore struct {
	state State
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: State{VotedFor: -1, Log: []rpc.LogEntry{}}}
}

func (m *MemoryStore) Save(state State) error {
	m.state = state
	return nil
}

func (m *MemoryStore) Load() (State, error) {
	if m.state.VotedFor == 0 && m.state.CurrentTerm == 0 && len(m.state.Log) == 0 && m.state.CommitIndex == 0 {
		return State{VotedFor: -1, Log: []rpc.LogEntry{}}, nil
	}
	return m.state, nil
}
