package main

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node    *maelstrom.Node
	mu      sync.Mutex
	counter int
}

func main() {
	serv := server{node: maelstrom.NewNode(), counter: 0}

	serv.node.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "generate_ok"
		var val int
		serv.mu.Lock()
		val = serv.counter
		serv.counter++
		serv.mu.Unlock()
		uniqId := serv.node.ID() + "_" + strconv.Itoa(val)
		body["id"] = uniqId
		return serv.node.Reply(msg, body)
	})
	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
