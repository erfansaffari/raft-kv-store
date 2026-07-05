package main

import (
	"flag"
	"log"
	"strings"

	"github.com/erfansaffari/raft-kv-store/internal/cluster"
)

func main() {
	id := flag.Int("id", 1, "node id")
	peers := flag.String("peers", "1,2,3", "comma-separated peer ids")
	clientAddr := flag.String("addr", ":9001", "client listen address")
	raftAddr := flag.String("raft-addr", ":9101", "raft peer listen address")
	peerRaft := flag.String("peer-raft", "1=localhost:9101,2=localhost:9102,3=localhost:9103", "id=host:port raft addresses")
	dataDir := flag.String("data", "", "data directory for persistence (optional)")
	flag.Parse()

	peerIDs, err := cluster.ParseIDs(*peers)
	if err != nil {
		log.Fatal(err)
	}
	peerRaftMap, err := cluster.ParsePeerRaft(*peerRaft)
	if err != nil {
		log.Fatal(err)
	}

	node, err := cluster.Start(cluster.Config{
		ID:         *id,
		PeerIDs:    peerIDs,
		ClientAddr: *clientAddr,
		RaftAddr:   *raftAddr,
		PeerRaft:   peerRaftMap,
		DataDir:    strings.TrimSpace(*dataDir),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("node-%d client=%s raft=%s", *id, node.ClientAddr(), node.RaftAddr())
	select {}
}
