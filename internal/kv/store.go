package kv

import "sync"

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Apply executes one command and returns the result.
func (s *Store) Apply(cmd Command) ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case "SET":
		s.data[cmd.Key] = cmd.Value
		return ApplyResult{}
	case "DELETE":
		_, found := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		return ApplyResult{Found: found}
	case "GET":
		value, ok := s.data[cmd.Key]
		return ApplyResult{Value: value, Found: ok}
	default:
		return ApplyResult{}
	}
}

// Snapshot returns a copy of the map (useful for tests and later snapshotting).
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copy := make(map[string]string, len(s.data))
	for k, v := range s.data {
		copy[k] = v
	}
	return copy
}
