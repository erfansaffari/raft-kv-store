package transport

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

type TCPRaftServer struct {
	mu       sync.Mutex
	listener net.Listener
	handler  func(msgType string, body json.RawMessage) (any, error)
}

func StartTCPRaftServer(addr string, handler func(msgType string, body json.RawMessage) (any, error)) (*TCPRaftServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &TCPRaftServer{listener: ln, handler: handler}
	go s.serve()
	return s, nil
}

func (s *TCPRaftServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *TCPRaftServer) Close() error {
	return s.listener.Close()
}

func (s *TCPRaftServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *TCPRaftServer) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := rpc.ReadMessage(conn)
		if err != nil {
			return
		}
		reply, err := s.handler(msg.Type, msg.Body)
		if err != nil {
			return
		}
		replyType := msg.Type + "_reply"
		if err := rpc.WriteMessage(conn, replyType, reply); err != nil {
			return
		}
	}
}

type TCPTransport struct {
	mu      sync.Mutex
	selfID  int
	peers   map[int]string
}

func NewTCPTransport(selfID int, peers map[int]string) *TCPTransport {
	return &TCPTransport{selfID: selfID, peers: peers}
}

func (t *TCPTransport) RequestVote(targetID int, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error) {
	var reply rpc.RequestVoteReply
	err := t.call(targetID, rpc.MsgRequestVote, args, &reply)
	return reply, err
}

func (t *TCPTransport) AppendEntries(targetID int, args rpc.AppendEntriesArgs) (rpc.AppendEntriesReply, error) {
	var reply rpc.AppendEntriesReply
	err := t.call(targetID, rpc.MsgAppendEntries, args, &reply)
	return reply, err
}

func (t *TCPTransport) call(targetID int, msgType string, args any, reply any) error {
	addr, ok := t.peers[targetID]
	if !ok {
		return fmt.Errorf("unknown peer %d", targetID)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := rpc.WriteMessage(conn, msgType, args); err != nil {
		return err
	}
	msg, err := rpc.ReadMessage(conn)
	if err != nil {
		return err
	}
	return json.Unmarshal(msg.Body, reply)
}
