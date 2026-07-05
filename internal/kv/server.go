package kv

import (
	"encoding/json"
	"net"

	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

type Server struct {
	store *Store
	ln    net.Listener
}

func NewServer(addr string) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{store: NewStore(), ln: ln}, nil
}

func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := rpc.ReadMessage(conn)
		if err != nil {
			return
		}
		switch msg.Type {
		case rpc.MsgClientRequest:
			var req rpc.ClientRequest
			if err := json.Unmarshal(msg.Body, &req); err != nil {
				return
			}
			result := s.store.Apply(req.Cmd)
			_ = rpc.WriteMessage(conn, rpc.MsgClientResponse, rpc.ClientResponse{
				OK:     true,
				Result: result,
			})
		}
	}
}

func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

func (s *Server) Close() error {
	return s.ln.Close()
}
