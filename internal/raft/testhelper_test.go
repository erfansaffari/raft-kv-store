package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/erfansaffari/raft-kv-store/internal/command"
	"github.com/erfansaffari/raft-kv-store/internal/kv"
	"github.com/erfansaffari/raft-kv-store/internal/persist"
	"github.com/erfansaffari/raft-kv-store/internal/raft"
	"github.com/erfansaffari/raft-kv-store/internal/transport"
)

type TestNode struct {
	ID     int
	Node   *raft.Node
	active bool
}

type TestCluster struct {
	t     *testing.T
	net   *transport.MemoryNetwork
	nodes []*TestNode
}

func startCluster(t *testing.T, size int) *TestCluster {
	t.Helper()
	net := transport.NewMemoryNetwork()
	cluster := &TestCluster{t: t, net: net}

	ids := make([]int, size)
	for i := 0; i < size; i++ {
		ids[i] = i + 1
	}

	for i := 0; i < size; i++ {
		id := ids[i]
		peers := make([]int, 0, size-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		store := kv.NewStore()
		tr := transport.NewMemoryTransport(net, id)
		node, err := raft.NewNode(id, peers, store, tr, persist.NewMemoryStore())
		if err != nil {
			t.Fatalf("NewNode: %v", err)
		}
		net.Register(id, node)
		node.Run()
		cluster.nodes = append(cluster.nodes, &TestNode{ID: id, Node: node, active: true})
	}
	return cluster
}

func startSimCluster(t *testing.T, size int) (*transport.SimNetwork, []*TestNode) {
	t.Helper()
	net := transport.NewSimNetwork()
	ids := make([]int, size)
	for i := 0; i < size; i++ {
		ids[i] = i + 1
	}

	var nodes []*TestNode
	for _, id := range ids {
		peers := make([]int, 0, size-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		store := kv.NewStore()
		node, err := raft.NewNode(id, peers, store, transport.NewSimTransport(net, id), persist.NewMemoryStore())
		if err != nil {
			t.Fatalf("NewNode: %v", err)
		}
		net.Register(id, node)
		node.Run()
		nodes = append(nodes, &TestNode{ID: id, Node: node, active: true})
	}
	return net, nodes
}

func (c *TestCluster) waitForLeader(timeout time.Duration) *TestNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if !n.active {
				continue
			}
			if n.Node.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (c *TestCluster) leaderCount() int {
	count := 0
	for _, n := range c.nodes {
		if n.active && n.Node.State() == raft.Leader {
			count++
		}
	}
	return count
}

func (c *TestCluster) submitSet(leader *TestNode, key, value string) error {
	_, err := leader.Node.SubmitCommand(command.Command{Op: "SET", Key: key, Value: value})
	return err
}

func (c *TestCluster) assertConsistent(t *testing.T) {
	t.Helper()
	var ref map[string]string
	first := true
	for _, n := range c.nodes {
		if !n.active {
			continue
		}
		snap := n.Node.Store().Snapshot()
		if first {
			ref = snap
			first = false
			continue
		}
		if fmt.Sprint(ref) != fmt.Sprint(snap) {
			t.Fatalf("node %d state %v != ref state %v", n.ID, snap, ref)
		}
	}
}

func (c *TestCluster) stopNode(id int) {
	c.net.Unregister(id)
	for _, n := range c.nodes {
		if n.ID == id {
			n.active = false
			n.Node.Stop()
		}
	}
}
