package main

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"time"
)

type WorkloadConfig struct {
	TotalOps     int
	KeyCount     int
	ValueSize    int
	ReadRatio    float64 // e.g., 0.8 for 80% read, 20% write
	WorkloadType string  // "put", "get", "delete", "mixed"
}

type OperationRequest struct {
	Op    string // "put", "get", "delete"
	Key   string
	Value string
}

func init() {
	mrand.Seed(time.Now().UnixNano())
}

func GenerateRandomValue(size int) string {
	bytes := make([]byte, size/2)
	crand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func GenerateWorkload(cfg WorkloadConfig) []OperationRequest {
	requests := make([]OperationRequest, cfg.TotalOps)

	for i := 0; i < cfg.TotalOps; i++ {
		keyId := mrand.Intn(cfg.KeyCount)
		key := fmt.Sprintf("key-%d", keyId)

		op := cfg.WorkloadType
		if cfg.WorkloadType == "mixed" {
			if mrand.Float64() < cfg.ReadRatio {
				op = "get"
			} else {
				op = "put"
			}
		}

		value := ""
		if op == "put" {
			value = GenerateRandomValue(cfg.ValueSize)
		}

		requests[i] = OperationRequest{
			Op:    op,
			Key:   key,
			Value: value,
		}
	}
	return requests
}