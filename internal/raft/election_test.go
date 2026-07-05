package raft_test

import (
	"testing"
	"time"
)

func TestElectsSingleLeader(t *testing.T) {
	cluster := startCluster(t, 3)
	leader := cluster.waitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	if cluster.leaderCount() != 1 {
		t.Fatalf("want 1 leader, got %d", cluster.leaderCount())
	}
}

func TestReElectAfterLeaderFailure(t *testing.T) {
	cluster := startCluster(t, 3)
	leader1 := cluster.waitForLeader(2 * time.Second)
	if leader1 == nil {
		t.Fatal("no initial leader")
	}
	cluster.stopNode(leader1.ID)

	leader2 := cluster.waitForLeader(5 * time.Second)
	if leader2 == nil {
		t.Fatal("no new leader after failure")
	}
	if leader2.ID == leader1.ID {
		t.Fatal("stopped node should not be leader")
	}
}
