package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node    *maelstrom.Node
	mu      sync.Mutex
	counter int
	kv      map[int]int
}

func updateKv(txns [][]any, kv map[int]int) [][]any {
	var results [][]any

	for _, txn := range txns {
		op := txn[0].(string)
		key := int(txn[1].(float64))
		if op == "r" {
			// Return txn with read value
			results = append(results, []any{op, key, kv[key]})
		} else if op == "w" {
			// Update KV and Return txn
			val := int(txn[2].(float64))
			kv[key] = val
			results = append(results, txn)
		}
	}

	return results
}

func (serv *server) handleTxn(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgId := body["msg_id"].(int)
	txns := body["txn"].([][]any)

	serv.mu.Lock()
	results := updateKv(txns, serv.kv)
	newMsgId := serv.counter
	serv.counter++
	serv.mu.Unlock()

	resBody := map[string]any{}
	resBody["msg_id"] = newMsgId
	resBody["txn"] = results
	resBody["type"] = "txn_ok"
	resBody["in_reply_to"] = msgId

	return serv.node.Reply(msg, "response")
}

func main() {
	serv := server{node: maelstrom.NewNode(), mu: sync.Mutex{}, counter: 0, kv: make(map[int]int)}

	serv.node.Handle("txn", serv.handleTxn)

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
