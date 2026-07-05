package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/erfansaffari/raft-kv-store/internal/command"
	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

func main() {
	addr := flag.String("addr", "localhost:9001", "server address")
	op := flag.String("op", "GET", "GET, SET, or DELETE")
	key := flag.String("key", "", "key")
	value := flag.String("value", "", "value (for SET)")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	req := rpc.ClientRequest{
		ID:  fmt.Sprintf("req-%d", time.Now().UnixNano()),
		Cmd: command.Command{Op: *op, Key: *key, Value: *value},
	}
	if err := rpc.WriteMessage(conn, rpc.MsgClientRequest, req); err != nil {
		log.Fatal(err)
	}

	msg, err := rpc.ReadMessage(conn)
	if err != nil {
		log.Fatal(err)
	}
	var resp rpc.ClientResponse
	if err := json.Unmarshal(msg.Body, &resp); err != nil {
		log.Fatal(err)
	}
	if !resp.OK {
		log.Fatalf("error: %s", resp.Error)
	}
	fmt.Printf("%+v\n", resp.Result)
}
