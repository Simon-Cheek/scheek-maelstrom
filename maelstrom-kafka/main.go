package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

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

func generateCommittedOffsetKey(key string) string {
	return "committedOffset/" + key
}

func (serv *server) handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets := body["offsets"].(map[string]any)
	returnMsg := map[string][][]int{}

	for key, offset := range offsets {
		returnMsg[key] = [][]int{}
		remainingMsgs := 5
		curOffset := int(offset.(float64))
		maxOffset, err := serv.kv.ReadInt(context.Background(), generateNextOffsetKey(key))
		if err != nil {
			return err
		}
		// Fetch up to 5 msgs starting at each offset
		for remainingMsgs > 0 && curOffset <= maxOffset {
			logEntryKey := generateLogEntryKey(key, curOffset)
			logMsg, logEntryErr := serv.kv.ReadInt(context.Background(), logEntryKey)
			if logEntryErr != nil {
				return logEntryErr
			}
			returnMsg[key] = append(returnMsg[key], []int{curOffset, logMsg})
			curOffset++
			remainingMsgs--
		}
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
	offsets = body["offsets"].(map[string]any)

	// Only update offset if value is greater than current
	for key, offset := range offsets {
		// Continually grab and attempt to CAS until successful
		for {
			curOffset, err := serv.kv.ReadInt(context.Background(), generateCommittedOffsetKey(key))
			if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}
			intOffset := int(offset.(float64))
			if curOffset > intOffset {
				break
			}
			casErr := serv.kv.CompareAndSwap(context.Background(), generateCommittedOffsetKey(key), curOffset, intOffset, true)
			if casErr == nil {
				break
			} else if maelstrom.ErrorCode(casErr) != maelstrom.PreconditionFailed {
				return casErr
			}
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

	serv.node.Handle("send", serv.handleSend)
	serv.node.Handle("poll", serv.handlePoll)
	serv.node.Handle("commit_offsets", serv.handleCommitOffsets)
	serv.node.Handle("list_committed_offsets", serv.handleListCommittedOffsets)

	if err := serv.node.Run(); err != nil {
		log.Fatal(err)
	}
}
