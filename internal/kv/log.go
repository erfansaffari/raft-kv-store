package kv

type Log struct {
	entries []Command
}

func NewLog() *Log {
	return &Log{entries: make([]Command, 0)}
}

func (l *Log) Append(cmd Command) int {
	l.entries = append(l.entries, cmd)
	return len(l.entries) // 1-based index
}

func (l *Log) Get(index int) (Command, bool) {
	if index < 1 || index > len(l.entries) {
		return Command{}, false
	}
	return l.entries[index-1], true
}

func (l *Log) Len() int {
	return len(l.entries)
}
