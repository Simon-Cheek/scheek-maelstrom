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

func updateKv(txns []any, kv map[int]int) [][]any {
	var results [][]any

	for _, txn := range txns {
		txnArr := txn.([]any)
		op := txnArr[0].(string)
		key := int(txnArr[1].(float64))
		if op == "r" {
			// Return txn with read value
			results = append(results, []any{op, key, kv[key]})
		} else if op == "w" {
			// Update KV and Return txn
			val := int(txnArr[2].(float64))
			kv[key] = val
			results = append(results, txnArr)
		}
	}

	return results
}

func (serv *server) handleTxn(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgId := int(body["msg_id"].(float64))
	txns := body["txn"].([]any)

	serv.mu.Lock()
	results := updateKv(txns, serv.kv)
	newMsgId := serv.counter
	serv.counter++
	serv.mu.Unlock()

	// Propagate to other nodes
	nodes := serv.node.NodeIDs()
	for _, node := range nodes {
		if node != serv.node.ID() {
			gossipMsg := map[string]any{}
			gossipMsg["type"] = "gossip"
			gossipMsg["msg_id"] = msgId
			gossipMsg["txn"] = txns
			sendErr := serv.node.Send(node, gossipMsg)
			if sendErr != nil {
				return sendErr
			}
		}
	}

	resBody := map[string]any{}
	resBody["msg_id"] = newMsgId
	resBody["txn"] = results
	resBody["type"] = "txn_ok"
	resBody["in_reply_to"] = msgId

	return serv.node.Reply(msg, resBody)
}

func (serv *server) handleGossip(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	txns := body["txn"].([]any)

	serv.mu.Lock()
	updateKv(txns, serv.kv)
	serv.mu.Unlock()

	return serv.node.Reply(msg, "")
}

func main() {
	serv := server{node: maelstrom.NewNode(), mu: sync.Mutex{}, counter: 0, kv: make(map[int]int)}

	serv.node.Handle("txn", serv.handleTxn)
	serv.node.Handle("gossip", serv.handleGossip)

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
