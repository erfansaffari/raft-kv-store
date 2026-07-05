package rpc

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/erfansaffari/raft-kv-store/internal/command"
)

func TestWriteReadMessage_ClientRequest(t *testing.T) {
	var buf bytes.Buffer

	req := ClientRequest{
		ID:  "req-1",
		Cmd: command.Command{Op: "SET", Key: "foo", Value: "bar"},
	}

	if err := WriteMessage(&buf, MsgClientRequest, req); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	msg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	if msg.Type != MsgClientRequest {
		t.Errorf("Type = %q, want %q", msg.Type, MsgClientRequest)
	}

	var got ClientRequest
	if err := json.Unmarshal(msg.Body, &got); err != nil {
		t.Fatalf("Unmarshal body: %v", err)
	}
	if got != req {
		t.Errorf("got %+v, want %+v", got, req)
	}
}

func TestWriteReadMessage_ClientResponse(t *testing.T) {
	var buf bytes.Buffer

	resp := ClientResponse{
		OK:     true,
		Result: command.ApplyResult{Value: "bar", Found: true},
	}

	if err := WriteMessage(&buf, MsgClientResponse, resp); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	msg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	if msg.Type != MsgClientResponse {
		t.Errorf("Type = %q, want %q", msg.Type, MsgClientResponse)
	}

	var got ClientResponse
	if err := json.Unmarshal(msg.Body, &got); err != nil {
		t.Fatalf("Unmarshal body: %v", err)
	}
	if got != resp {
		t.Errorf("got %+v, want %+v", got, resp)
	}
}

func TestWriteReadMessage_MultipleMessages(t *testing.T) {
	var buf bytes.Buffer

	messages := []struct {
		msgType string
		body    any
	}{
		{MsgClientRequest, ClientRequest{ID: "1", Cmd: command.Command{Op: "GET", Key: "a"}}},
		{MsgClientResponse, ClientResponse{OK: true, Result: command.ApplyResult{Found: false}}},
		{MsgRequestVote, RequestVoteArgs{Term: 2, CandidateID: 1, LastLogIndex: 3, LastLogTerm: 1}},
	}

	for _, m := range messages {
		if err := WriteMessage(&buf, m.msgType, m.body); err != nil {
			t.Fatalf("WriteMessage(%q): %v", m.msgType, err)
		}
	}

	for _, want := range messages {
		msg, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if msg.Type != want.msgType {
			t.Errorf("Type = %q, want %q", msg.Type, want.msgType)
		}

		wantBody, err := json.Marshal(want.body)
		if err != nil {
			t.Fatalf("Marshal want body: %v", err)
		}
		if string(msg.Body) != string(wantBody) {
			t.Errorf("Body = %s, want %s", msg.Body, wantBody)
		}
	}
}

func TestReadMessage_TruncatedHeader(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0, 0, 0}) // only 3 bytes, need 4

	_, err := ReadMessage(buf)
	if err == nil {
		t.Fatal("expected error for truncated header")
	}
}
