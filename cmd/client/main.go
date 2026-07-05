package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/erfansaffari/raft-kv-store/internal/command"
	"github.com/erfansaffari/raft-kv-store/internal/rpc"
)

func main() {
	addrs := flag.String("cluster", "localhost:9001,localhost:9002,localhost:9003", "comma-separated client addresses")
	addr := flag.String("addr", "", "single server address (overrides cluster)")
	op := flag.String("op", "", "GET, SET, or DELETE")
	key := flag.String("key", "", "key")
	value := flag.String("value", "", "value (for SET)")
	repl := flag.Bool("repl", false, "interactive REPL mode")
	flag.Parse()

	if *repl || (*op == "" && *key == "") {
		runREPL(parseAddrs(*addrs))
		return
	}

	targets := parseAddrs(*addrs)
	if *addr != "" {
		targets = []string{*addr}
	}
	result, err := execute(targets, *op, *key, *value)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", result)
}

func parseAddrs(spec string) []string {
	parts := strings.Split(spec, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func runREPL(addrs []string) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("raft-kv REPL — commands: SET key val | GET key | DELETE key | quit")
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "quit" || line == "exit" {
			return
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			fmt.Println("usage: OP key [value]")
			continue
		}
		op := strings.ToUpper(fields[0])
		key := fields[1]
		val := ""
		if len(fields) > 2 {
			val = fields[2]
		}
		result, err := execute(addrs, op, key, val)
		if err != nil {
			fmt.Println("error:", err)
			continue
		}
		fmt.Printf("%+v\n", result)
	}
}

func execute(addrs []string, op, key, value string) (command.ApplyResult, error) {
	var lastErr error
	for _, addr := range addrs {
		result, leaderID, err := tryOnce(addr, op, key, value)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if leaderID > 0 {
			leaderAddr := fmt.Sprintf("localhost:%d", 9000+leaderID)
			if result, _, err := tryOnce(leaderAddr, op, key, value); err == nil {
				return result, nil
			}
		}
	}
	return command.ApplyResult{}, lastErr
}

func tryOnce(addr, op, key, value string) (command.ApplyResult, int, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return command.ApplyResult{}, 0, err
	}
	defer conn.Close()

	req := rpc.ClientRequest{
		ID:  fmt.Sprintf("req-%d", time.Now().UnixNano()),
		Cmd: command.Command{Op: strings.ToUpper(op), Key: key, Value: value},
	}
	if err := rpc.WriteMessage(conn, rpc.MsgClientRequest, req); err != nil {
		return command.ApplyResult{}, 0, err
	}

	msg, err := rpc.ReadMessage(conn)
	if err != nil {
		return command.ApplyResult{}, 0, err
	}
	var resp rpc.ClientResponse
	if err := json.Unmarshal(msg.Body, &resp); err != nil {
		return command.ApplyResult{}, 0, err
	}
	if !resp.OK {
		leaderID, _ := strconv.Atoi(resp.LeaderID)
		return command.ApplyResult{}, leaderID, fmt.Errorf("%s", resp.Error)
	}
	return resp.Result, 0, nil
}
