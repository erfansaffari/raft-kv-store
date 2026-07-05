package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/erfansaffari/raft-kv-store/internal/command"
	"github.com/erfansaffari/raft-kv-store/internal/kv"
	"github.com/erfansaffari/raft-kv-store/internal/persist"
	"github.com/erfansaffari/raft-kv-store/internal/raft"
	"github.com/erfansaffari/raft-kv-store/internal/rpc"
	"github.com/erfansaffari/raft-kv-store/internal/transport"
)

type Node struct {
	ID       int
	Raft     *raft.Node
	clientLn net.Listener
	raftSrv  *transport.TCPRaftServer
}

type Config struct {
	ID         int
	PeerIDs    []int
	ClientAddr string
	RaftAddr   string
	PeerRaft   map[int]string
	DataDir    string
}

func Start(cfg Config) (*Node, error) {
	store := kv.NewStore()

	var p persist.Store = persist.NewMemoryStore()
	if cfg.DataDir != "" {
		fileStore, err := persist.NewFileStore(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		p = fileStore
	}

	peers := make([]int, 0, len(cfg.PeerIDs))
	for _, id := range cfg.PeerIDs {
		if id != cfg.ID {
			peers = append(peers, id)
		}
	}

	tcp := transport.NewTCPTransport(cfg.ID, cfg.PeerRaft)
	raftNode, err := raft.NewNode(cfg.ID, peers, store, tcp, p)
	if err != nil {
		return nil, err
	}

	n := &Node{ID: cfg.ID, Raft: raftNode}
	raftNode.Run()

	raftSrv, err := transport.StartTCPRaftServer(cfg.RaftAddr, func(msgType string, body json.RawMessage) (any, error) {
		switch msgType {
		case rpc.MsgRequestVote:
			var args rpc.RequestVoteArgs
			if err := json.Unmarshal(body, &args); err != nil {
				return nil, err
			}
			return raftNode.HandleRequestVote(args), nil
		case rpc.MsgAppendEntries:
			var args rpc.AppendEntriesArgs
			if err := json.Unmarshal(body, &args); err != nil {
				return nil, err
			}
			return raftNode.HandleAppendEntries(args), nil
		default:
			return nil, fmt.Errorf("unknown raft msg %s", msgType)
		}
	})
	if err != nil {
		return nil, err
	}
	n.raftSrv = raftSrv

	clientLn, err := net.Listen("tcp", cfg.ClientAddr)
	if err != nil {
		return nil, err
	}
	n.clientLn = clientLn
	go n.serveClients()

	return n, nil
}

func (n *Node) ClientAddr() string {
	return n.clientLn.Addr().String()
}

func (n *Node) RaftAddr() string {
	return n.raftSrv.Addr()
}

func (n *Node) Close() error {
	n.Raft.Stop()
	_ = n.raftSrv.Close()
	return n.clientLn.Close()
}

func (n *Node) serveClients() {
	for {
		conn, err := n.clientLn.Accept()
		if err != nil {
			return
		}
		go n.handleClient(conn)
	}
}

func (n *Node) handleClient(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := rpc.ReadMessage(conn)
		if err != nil {
			return
		}
		if msg.Type != rpc.MsgClientRequest {
			continue
		}
		var req rpc.ClientRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return
		}

		var result command.ApplyResult
		var reqErr error

		switch req.Cmd.Op {
		case "GET":
			result, reqErr = n.Raft.Get(req.Cmd.Key)
		default:
			result, reqErr = n.Raft.SubmitCommand(req.Cmd)
		}

		resp := rpc.ClientResponse{OK: true, Result: result}
		if reqErr != nil {
			var notLeader *raft.NotLeaderError
			if errors.As(reqErr, &notLeader) {
				resp.OK = false
				resp.Error = "not leader"
				if notLeader.LeaderID > 0 {
					resp.LeaderID = strconv.Itoa(notLeader.LeaderID)
				}
			} else {
				resp.OK = false
				resp.Error = reqErr.Error()
			}
		}

		_ = rpc.WriteMessage(conn, rpc.MsgClientResponse, resp)
	}
}

func ParsePeerRaft(spec string) (map[int]string, error) {
	out := make(map[int]string)
	if spec == "" {
		return out, nil
	}
	for _, part := range strings.Split(spec, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer raft %q", part)
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil {
			return nil, err
		}
		out[id] = kv[1]
	}
	return out, nil
}

func ParseIDs(spec string) ([]int, error) {
	parts := strings.Split(spec, ",")
	ids := make([]int, 0, len(parts))
	for _, p := range parts {
		id, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
