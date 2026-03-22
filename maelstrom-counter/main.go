package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node     *maelstrom.Node
	mu       sync.Mutex
	messages []int
	topology map[string][]string
	kv       *maelstrom.KV
}

func (serv *server) handleAdd(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	delta := int(body["delta"].(float64))
	cur, err := serv.kv.ReadInt(context.Background(), serv.node.ID())
	var val int
	if err != nil {
		val = delta
	} else {
		val = cur + delta
	}
	if err := serv.kv.Write(context.Background(), serv.node.ID(), val); err != nil {
		return err
	}
	response := map[string]any{}
	response["type"] = "add_ok"
	return serv.node.Reply(msg, response)
}

func (serv *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	serv.mu.Lock()
	defer serv.mu.Unlock()
	body["messages"] = serv.messages
	body["type"] = "read_ok"
	return serv.node.Reply(msg, body)
}

func (serv *server) handleTopology(msg maelstrom.Message) error {
	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	serv.mu.Lock()
	defer serv.mu.Unlock()
	serv.topology = body.Topology
	response := map[string]any{}
	response["type"] = "topology_ok"
	return serv.node.Reply(msg, response)
}

func main() {
	serv := server{node: maelstrom.NewNode(), messages: []int{}}
	serv.kv = maelstrom.NewSeqKV(serv.node)

	serv.node.Handle("add", serv.handleAdd)
	serv.node.Handle("read", serv.handleRead)
	serv.node.Handle("topology", serv.handleTopology)

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
