// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	saramaPublisherConfig := kafka.DefaultSaramaSyncPublisherConfig()

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers: []string{"localhost:9092"},
			Marshaler: kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
				return msg.Metadata.Get("partition"), nil
			}),
			OverwriteSaramaConfig: saramaPublisherConfig,
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	rand.Seed(time.Now().UnixNano())

	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))
		msg.Metadata.Set("partition", "XXX"+strconv.Itoa(rand.Intn(2)+1))

		if err := publisher.Publish("experiment.topic", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}
