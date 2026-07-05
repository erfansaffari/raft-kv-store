package raft_test

import (
	"errors"
	"testing"
	"time"

	"github.com/erfansaffari/raft-kv-store/internal/command"
	"github.com/erfansaffari/raft-kv-store/internal/raft"
)

func TestBasicReplication(t *testing.T) {
	cluster := startCluster(t, 3)
	leader := cluster.waitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	if err := cluster.submitSet(leader, "x", "1"); err != nil {
		t.Fatalf("submit SET: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	cluster.assertConsistent(t)

	result, err := leader.Node.Get("x")
	if err != nil {
		t.Fatalf("leader get: %v", err)
	}
	if !result.Found || result.Value != "1" {
		t.Fatalf("got %+v", result)
	}
}

func TestWriteAfterLeaderChange(t *testing.T) {
	cluster := startCluster(t, 3)
	leader1 := cluster.waitForLeader(2 * time.Second)
	if leader1 == nil {
		t.Fatal("no leader")
	}
	cluster.stopNode(leader1.ID)
	time.Sleep(100 * time.Millisecond)

	leader2 := cluster.waitForLeader(5 * time.Second)
	if leader2 == nil {
		t.Fatal("no new leader")
	}

	if err := cluster.submitSet(leader2, "y", "2"); err != nil {
		t.Fatalf("submit after re-election: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	cluster.assertConsistent(t)
}

func TestNotLeaderRedirect(t *testing.T) {
	cluster := startCluster(t, 3)
	leader := cluster.waitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	var follower *TestNode
	for _, n := range cluster.nodes {
		if n.ID != leader.ID {
			follower = n
			break
		}
	}

	_, err := follower.Node.SubmitCommand(command.Command{Op: "SET", Key: "a", Value: "b"})
	var notLeader *raft.NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("expected NotLeaderError, got %v", err)
	}
}
