package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type logEntry struct {
	offset int
	msg    float64
}

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func generateNextOffsetKey(key string) string {
	return "nextOffset/" + key
}

func generateLogEntryKey(key string, offset int) string {
	return "log/" + key + "/" + strconv.Itoa(offset)
}

func (serv *server) handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets := body["offsets"].(map[string]any)
	returnMsg := map[string][][]int{}

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

	nextOffsetKey := generateNextOffsetKey(key)
	var cur int
	var err error

	// Retry until Offset claimed
	for {
		cur, err = serv.kv.ReadInt(context.Background(), nextOffsetKey)
		if err != nil && maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			cur = 0
		} else if err != nil {
			return err
		}
		err = serv.kv.CompareAndSwap(context.Background(), nextOffsetKey, cur, cur+1, true)
		if err == nil {
			break
		} else if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
			return err
		}
	}

	// Write to log
	writeErr := serv.kv.Write(context.Background(), generateLogEntryKey(key, cur), receivedMsg)
	if writeErr != nil {
		return writeErr
	}

	resBody := map[string]any{}
	resBody["type"] = "send_ok"
	resBody["offset"] = cur
	return serv.node.Reply(msg, resBody)
}

func (serv *server) handleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	var offsets map[string]any
	serv.logMutex.Lock()
	defer serv.logMutex.Unlock()
	offsets = body["offsets"].(map[string]any)
	for key, offset := range offsets {
		intOffset := int(offset.(float64))
		prevOffset := serv.committedOffsets[key]
		if intOffset > prevOffset {
			serv.committedOffsets[key] = intOffset
		}
	}
	resBody := map[string]any{}
	resBody["type"] = "commit_offsets_ok"
	return serv.node.Reply(msg, resBody)
}

func (serv *server) handleListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	keys := body["keys"].([]any)
	offsets := map[string]int{}
	serv.logMutex.RLock()
	defer serv.logMutex.RUnlock()
	for _, key := range keys {
		offsets[key.(string)] = serv.committedOffsets[key.(string)]
	}
	resBody := map[string]any{}
	resBody["type"] = "list_committed_offsets_ok"
	resBody["offsets"] = offsets
	return serv.node.Reply(msg, resBody)
}

func main() {
	serv := server{node: maelstrom.NewNode()}
	serv.kv = maelstrom.NewLinKV(serv.node)
	serv.logCounter = 0
	serv.logMutex = sync.RWMutex{}
	serv.logs = make(map[string][]logEntry)
	serv.committedOffsets = map[string]int{}

	serv.node.Handle("send", serv.handleSend)
	serv.node.Handle("poll", serv.handlePoll)
	serv.node.Handle("commit_offsets", serv.handleCommitOffsets)
	serv.node.Handle("list_committed_offsets", serv.handleListCommittedOffsets)

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
