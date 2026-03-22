package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node     *maelstrom.Node
	mu       sync.Mutex
	messages []int
	topology map[string]any
}

//func (serv *server) handleGenerate(msg maelstrom.Message) error {
//	var body map[string]any
//	if err := json.Unmarshal(msg.Body, &body); err != nil {
//		return err
//	}
//	body["type"] = "generate_ok"
//	var val int
//	serv.mu.Lock()
//	val = serv.counter
//	serv.counter++
//	serv.mu.Unlock()
//	uniqId := serv.node.ID() + "_" + strconv.Itoa(val)
//	body["id"] = uniqId
//	return serv.node.Reply(msg, body)
//}

func (serv *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	msgContent := int(body["message"].(float64))
	serv.mu.Lock()
	defer serv.mu.Unlock()
	serv.messages = append(serv.messages, msgContent)
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
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	serv.mu.Lock()
	defer serv.mu.Unlock()
	serv.topology = body["topology"].(map[string]any)
	response := map[string]any{}
	response["type"] = "topology_ok"
	return serv.node.Reply(msg, response)
}

func main() {
	serv := server{node: maelstrom.NewNode(), messages: []int{}}

	//serv.node.Handle("generate", serv.handleGenerate)
	serv.node.Handle("broadcast", serv.handleBroadcast)
	serv.node.Handle("read", serv.handleRead)
	serv.node.Handle("topology", serv.handleTopology)

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
