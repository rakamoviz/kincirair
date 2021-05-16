package main

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
)

const (
	partitions        = 2
	maxProcessingTime = 2 * time.Second
	slackTime         = 5 * time.Second
)

type CqrsMessage interface {
	GetCqrsAggregateId() string
	GetCqrsAggregateName() string
	GetPartitionKey() string
}

func (bookRoomCmd *BookRoom) GetCqrsAggregateId() string {
	return bookRoomCmd.RoomId
}

func (bookRoomCmd *BookRoom) GetCqrsAggregateName() string {
	return bookRoomCmd.RoomId
}

func (bookRoomCmd *BookRoom) GetPartitionKey() string {
	return bookRoomCmd.RoomId
}

type PartitionedCommandEventMarshaler struct {
	delegate cqrs.CommandEventMarshaler
}

func (m PartitionedCommandEventMarshaler) Marshal(v interface{}) (*message.Message, error) {
	msg, err := m.delegate.Marshal(v)

	if err == nil {
		msg.Metadata.Set("partition", v.(CqrsMessage).GetPartitionKey())
	}

	return msg, err
}

func (m PartitionedCommandEventMarshaler) Unmarshal(msg *message.Message, v interface{}) (err error) {
	return m.delegate.Unmarshal(msg, v)
}

func (m PartitionedCommandEventMarshaler) Name(v interface{}) string {
	return m.delegate.Name(v)
}

func (m PartitionedCommandEventMarshaler) NameFromMessage(msg *message.Message) string {
	return m.delegate.NameFromMessage(msg)
}

func main() {
	cqrsMarshaler := PartitionedCommandEventMarshaler{cqrs.ProtobufMarshaler{}}

	commandsPublisherConfig := kafka.DefaultSaramaSyncPublisherConfig()
	commandsPublisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers: []string{"localhost:9092"},
			Marshaler: kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
				return msg.Metadata.Get("partition"), nil
			}),
			OverwriteSaramaConfig: commandsPublisherConfig,
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	commandBus, err := cqrs.NewCommandBus(
		commandsPublisher,
		func(commandName string) string {
			return commandName
		},
		cqrsMarshaler,
	)
	if err != nil {
		panic(err)
	}

	// publish BookRoom commands every second to simulate incoming traffic
	publishCommands(commandBus)
}

func publishCommands(commandBus *cqrs.CommandBus) func() {
	i := 0
	for {
		i++

		startDate, err := ptypes.TimestampProto(time.Now())
		if err != nil {
			panic(err)
		}

		endDate, err := ptypes.TimestampProto(time.Now().Add(time.Hour * 24 * 3))
		if err != nil {
			panic(err)
		}

		bookRoomCmd := &BookRoom{
			RoomId:    fmt.Sprintf("%d", i),
			GuestName: "John",
			StartDate: startDate,
			EndDate:   endDate,
		}
		if err := commandBus.Send(context.Background(), bookRoomCmd); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}
