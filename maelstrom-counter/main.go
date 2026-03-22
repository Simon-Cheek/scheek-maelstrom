package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
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
	totalVal := 0
	nodeIds := serv.node.NodeIDs()
	for _, id := range nodeIds {
		cur, err := serv.kv.ReadInt(context.Background(), id)
		if err == nil {
			totalVal += cur
		}
	}

	body["type"] = "read_ok"
	body["value"] = totalVal
	return serv.node.Reply(msg, body)
}

func main() {
	serv := server{node: maelstrom.NewNode()}
	serv.kv = maelstrom.NewLinKV(serv.node)

	serv.node.Handle("add", serv.handleAdd)
	serv.node.Handle("read", serv.handleRead)

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
