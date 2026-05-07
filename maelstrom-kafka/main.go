package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type logEntry struct {
	offset int
	msg    float64
}

type server struct {
	node *maelstrom.Node
	//kv   *maelstrom.KV
	logs       map[string][]logEntry
	logCounter int
	logMutex   sync.RWMutex
}

func (serv *server) handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets := body["offsets"].(map[string]any)
	returnMsg := map[string][][]int{}
	serv.logMutex.RLock()
	defer serv.logMutex.RUnlock()
	for key, offset := range offsets {
		var logsToReturn []logEntry
		logArrs := [][]int{}
		var logEnts []logEntry = serv.logs[key]
		for _, logEnt := range logEnts {
			if logEnt.offset >= int(offset.(float64)) {
				logsToReturn = append(logsToReturn, logEnt)
			}
		}
		for _, logToReturn := range logsToReturn {
			logArr := []int{logToReturn.offset, int(logToReturn.msg)}
			logArrs = append(logArrs, logArr)
		}
		returnMsg[key] = logArrs
	}
	resp := map[string]any{}
	resp["type"] = "poll_ok"
	resp["msgs"] = returnMsg
	return serv.node.Reply(msg, resp)
}

func (serv *server) handleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	key := body["key"].(string)
	receivedMsg := body["msg"].(float64)
	serv.logMutex.Lock()
	defer serv.logMutex.Unlock()
	serv.logCounter++
	count := serv.logCounter
	serv.logs[key] = append(serv.logs[key], logEntry{count, receivedMsg})

	resBody := map[string]any{}
	resBody["type"] = "send_ok"
	resBody["offset"] = count

	return serv.node.Reply(msg, resBody)
}

func (serv *server) handleCommitOffsets(msg maelstrom.Message) error {
	//var body map[string]any
	//if err := json.Unmarshal(msg.Body, &body); err != nil {
	//	return err
	//}
	//totalVal := 0
	//nodeIds := serv.node.NodeIDs()
	//for _, id := range nodeIds {
	//	cur, err := serv.kv.ReadInt(context.Background(), id)
	//	if err == nil {
	//		totalVal += cur
	//	}
	//}
	//
	//body["type"] = "read_ok"
	//body["value"] = totalVal
	//return serv.node.Reply(msg, body)
	return nil
}

func (serv *server) handleListCommittedOffsets(msg maelstrom.Message) error {
	//var body map[string]any
	//if err := json.Unmarshal(msg.Body, &body); err != nil {
	//	return err
	//}
	//totalVal := 0
	//nodeIds := serv.node.NodeIDs()
	//for _, id := range nodeIds {
	//	cur, err := serv.kv.ReadInt(context.Background(), id)
	//	if err == nil {
	//		totalVal += cur
	//	}
	//}
	//
	//body["type"] = "read_ok"
	//body["value"] = totalVal
	//return serv.node.Reply(msg, body)
	return nil
}

func main() {
	serv := server{node: maelstrom.NewNode()}
	serv.logCounter = 0

	serv.node.Handle("send", serv.handleSend)
	serv.node.Handle("poll", serv.handlePoll)
	serv.node.Handle("commit_offsets", serv.handleCommitOffsets)
	serv.node.Handle("list_committed_offsets", serv.handleListCommittedOffsets)

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
