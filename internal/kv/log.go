package kv

import "github.com/erfansaffari/raft-kv-store/internal/command"

type Log struct {
	entries []command.Command
}

func NewLog() *Log {
	return &Log{entries: make([]command.Command, 0)}
}

func (l *Log) Append(cmd command.Command) int {
	l.entries = append(l.entries, cmd)
	return len(l.entries)
}

func (l *Log) Get(index int) (command.Command, bool) {
	if index < 1 || index > len(l.entries) {
		return command.Command{}, false
	}
	return l.entries[index-1], true
}

func (l *Log) Len() int {
	return len(l.entries)
}
