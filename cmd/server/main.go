package main

import (
	"flag"
	"log"

	"github.com/erfansaffari/raft-kv-store/internal/kv"
)

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	flag.Parse()

	srv, err := kv.NewServer(*addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("single-node KV server listening on %s", srv.Addr())
	log.Fatal(srv.Serve())
}
