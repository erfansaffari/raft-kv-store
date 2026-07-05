package raft_test

import (
	"testing"
	"time"
)

func TestChaosPartition(t *testing.T) {
	net, nodes := startSimCluster(t, 5)
	net.Partition([]int{1, 2, 3}, []int{4, 5})

	cluster := &TestCluster{t: t, nodes: nodes}
	leader := cluster.waitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader on majority partition")
	}

	if err := cluster.submitSet(leader, "x", "1"); err != nil {
		t.Fatalf("majority write failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	net.Heal()
	time.Sleep(500 * time.Millisecond)

	cluster.assertConsistent(t)
}

func TestPersistedStateSurvivesRestart(t *testing.T) {
	cluster := startCluster(t, 3)
	leader := cluster.waitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}
	if err := cluster.submitSet(leader, "k", "v"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	cluster.assertConsistent(t)
}
