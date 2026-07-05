package command

// Command is one operation in the replicated log.
// Raft doesn't interpret these — it just replicates them.
// The state machine (KV store) applies them in log order.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ApplyResult is returned when a command is applied to the state machine.
type ApplyResult struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found,omitempty"`
}
