package raft

import "encoding/json"

type Command struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

func encodeCommand(
	operation string,
	key string,
	value string,
) ([]byte, error) {

	command := Command{
		Operation: operation,
		Key:       key,
		Value:     value,
	}

	return json.Marshal(command)
}

func decodeCommand(
	data []byte,
) (Command, error) {

	var command Command

	err := json.Unmarshal(
		data,
		&command,
	)

	return command, err
}
