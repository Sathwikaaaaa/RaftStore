package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "raftstore/proto"
)

func main() {

	address := flag.String(
		"address",
		"localhost:5001",
		"Raft server address",
	)

	operation := flag.String(
		"op",
		"get",
		"Operation: put, get, delete",
	)

	key := flag.String(
		"key",
		"",
		"Key",
	)

	value := flag.String(
		"value",
		"",
		"Value",
	)

	flag.Parse()

	conn, err := grpc.Dial(
		*address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {

		log.Fatal(err)
	}

	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	defer cancel()

	switch *operation {

	case "put":

		resp, err := client.Put(
			ctx,
			&pb.PutRequest{
				Key:   *key,
				Value: *value,
			},
		)

		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf(
			"PUT response: success=%v leader=%s message=%s\n",
			resp.Success,
			resp.LeaderId,
			resp.Message,
		)

	case "get":

		resp, err := client.Get(
			ctx,
			&pb.GetRequest{
				Key: *key,
			},
		)

		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf(
			"GET response: found=%v value=%q leader=%s message=%s\n",
			resp.Found,
			resp.Value,
			resp.LeaderId,
			resp.Message,
		)

	case "delete":

		resp, err := client.Delete(
			ctx,
			&pb.DeleteRequest{
				Key: *key,
			},
		)

		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf(
			"DELETE response: success=%v leader=%s message=%s\n",
			resp.Success,
			resp.LeaderId,
			resp.Message,
		)

	default:

		log.Fatalf(
			"unknown operation: %s",
			*operation,
		)
	}
}
