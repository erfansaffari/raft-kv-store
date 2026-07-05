package persist

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

type FileStore struct {
	path string
}

func NewFileStore(dataDir string) (*FileStore, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{path: filepath.Join(dataDir, "raft_state.json")}, nil
}

func (f *FileStore) Save(state State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}

func (f *FileStore) Load() (State, error) {
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return State{VotedFor: -1, Log: []rpc.LogEntry{}}, nil
	}
	if err != nil {
		return State{}, err
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, err
	}
	if state.Log == nil {
		state.Log = []rpc.LogEntry{}
	}
	return state, nil
}
