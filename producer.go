package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "input"
	brokerAddress = "localhost:9092"
)

func main() {
	// create a new context
	ctx := context.Background()
	produce(ctx)
}

func produce(ctx context.Context) {

	// initialize a counter
	i := 0
	l := log.New(os.Stdout, "kafka writer: ", 0)

	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		// assign the logger to the writer
		Logger: l,
	})
	b := make([]byte, 8)

	//a := makeTimestamp()
	for {
		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		// the message gets published on
		binary.LittleEndian.PutUint64(b, uint64(time.Now().Unix()))
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			//Value: []byte("this is message" + strconv.Itoa(i) + " --- " + strconv.FormatInt(makeTimestamp(), 2)),
			Value: b,
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes:", i)
		i++
		// sleep for a second
		time.Sleep(time.Second)
	}
}

