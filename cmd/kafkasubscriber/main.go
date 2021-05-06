// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	partitions        = 2
	maxProcessingTime = 2 * time.Second
	slackTime         = 5 * time.Second
)

func main() {
	saramaSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaSubscriberConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               []string{"localhost:9092"},
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: configsByProcessingTime(saramaSubscriberConfig),
			ConsumerGroup:         "test_consumer_group",
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "experiment.topic")
	if err != nil {
		panic(err)
	}

	process(messages)
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf(
			"received message on partition %s: %s, payload: %s",
			msg.Metadata.Get("partition"), msg.UUID, string(msg.Payload),
		)

		time.Sleep(2 * time.Second)

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}

func configsByProcessingTime(conf *sarama.Config) *sarama.Config {
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.ChannelBufferSize = 0
	conf.Consumer.MaxProcessingTime = maxProcessingTime * partitions
	conf.Consumer.Group.Rebalance.Timeout = (maxProcessingTime * partitions) + slackTime
	conf.Net.ReadTimeout = (maxProcessingTime * partitions) + slackTime
	conf.Net.WriteTimeout = (maxProcessingTime * partitions) + slackTime
	conf.Net.DialTimeout = (maxProcessingTime * partitions) + slackTime

	return conf
}
