package rpc

import (
	"encoding/json"
	"io"
)

type Message struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

const (
	MsgClientRequest  = "client_request"
	MsgClientResponse = "client_response"
	// Raft types added in Part 2:
	MsgRequestVote        = "request_vote"
	MsgRequestVoteReply   = "request_vote_reply"
	MsgAppendEntries      = "append_entries"
	MsgAppendEntriesReply = "append_entries_reply"
)

func WriteMessage(w io.Writer, msgType string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	msg := Message{Type: msgType, Body: raw}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	// Length-prefix framing: 4-byte big-endian length + JSON
	header := []byte{
		byte(len(data) >> 24),
		byte(len(data) >> 16),
		byte(len(data) >> 8),
		byte(len(data)),
	}
	if _, err := w.Write(append(header, data...)); err != nil {
		return err
	}
	return nil
}

func ReadMessage(r io.Reader) (Message, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return Message{}, err
	}
	length := int(header[0])<<24 | int(header[1])<<16 | int(header[2])<<8 | int(header[3])
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return Message{}, err
	}
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return Message{}, err
	}
	return msg, nil
}
