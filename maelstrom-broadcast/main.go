package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node     *maelstrom.Node
	mu       sync.Mutex
	messages []int
	topology map[string][]string
}

func (serv *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	msgContent := int(body["message"].(float64))
	serv.mu.Lock()
	defer serv.mu.Unlock()

	if slices.Contains(serv.messages, msgContent) {
		// Ignore, already received
	} else {
		// Gossip!

		// Make message
		broadcastMsg := map[string]any{}
		broadcastMsg["message"] = msgContent
		broadcastMsg["type"] = "broadcast"

		serv.messages = append(serv.messages, msgContent)
		if serv.topology == nil {
			serv.topology = make(map[string][]string)
			allNodes := serv.node.NodeIDs()

			// Send to EVERYONE if no topology
			for _, node := range allNodes {
				if node != serv.node.ID() {
					serv.node.Send(node, broadcastMsg)
				}
			}
		} else {
			// Just send to neighbors
			neighbors := serv.topology[serv.node.ID()]
			if neighbors != nil {
				for _, neighbor := range neighbors {
					serv.node.Send(neighbor, broadcastMsg)
				}
			}
		}
	}
	response := map[string]any{}
	response["type"] = "broadcast_ok"
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

	serv.node.Handle("broadcast", serv.handleBroadcast)
	serv.node.Handle("read", serv.handleRead)
	serv.node.Handle("topology", serv.handleTopology)
	serv.node.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
