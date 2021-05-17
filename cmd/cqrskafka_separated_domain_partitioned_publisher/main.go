package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/types/known/timestamppb"

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

func FullyQualifiedStructName(v interface{}) string {
	s := fmt.Sprintf("%T", v)
	s = strings.TrimLeft(s, "*")

	return s
}

type Aggregate interface {
	GetAggregateId() string
	GetAggregateName() string
}

type Reservation struct {
	Id        string
	RoomId    string
	GuestName string
	Price     int64
	StartDate time.Time
	EndDate   time.Time
}

func (r Reservation) GetAggregateId() string {
	return r.Id
}

func (r Reservation) GetAggregateName() string {
	return FullyQualifiedStructName(r)
}

type CqrsMessage interface {
	CqrsHeader() *CqrsHeader
	GetPartitionKey() string
}

func NewCqrsHeader(
	trigger CqrsMessage, messageType CqrsMessageType,
	aggregate Aggregate, processId string, processName string,
) *CqrsHeader {
	id := watermill.NewUUID()
	correlationId := ""

	if trigger != nil {
		correlationId = trigger.CqrsHeader().CorrelationId
	}
	if correlationId == "" {
		correlationId = id
	}

	if processId == "" {
		if trigger != nil {
			processId = trigger.CqrsHeader().ProcessId
			processName = trigger.CqrsHeader().ProcessName
		}
	}

	return &CqrsHeader{
		Type:          messageType,
		Domain:        "kincirair",
		Id:            id,
		CorrelationId: correlationId,
		AggregateName: FullyQualifiedStructName(aggregate),
		ProcessId:     processId,
		ProcessName:   processName,
	}
}

func NewBookRoom(
	trigger CqrsMessage, roomId string, guestName string,
	startDate *timestamppb.Timestamp,
	endDate *timestamppb.Timestamp,
) *BookRoom {
	return &BookRoom{
		Header:    NewCqrsHeader(trigger, CqrsMessageType_COMMAND, Reservation{}, "", ""),
		RoomId:    roomId,
		GuestName: guestName,
		StartDate: startDate,
		EndDate:   endDate,
	}
}

func (bookRoomCmd *BookRoom) CqrsHeader() *CqrsHeader {
	return bookRoomCmd.Header
}

func (bookRoomCmd *BookRoom) GetPartitionKey() string {
	return bookRoomCmd.RoomId
}

type PartitionedCommandEventMarshaler struct {
	delegate cqrs.CommandEventMarshaler
}

func (m PartitionedCommandEventMarshaler) Marshal(v interface{}) (*message.Message, error) {
	cqrsMessage := v.(CqrsMessage)

	sentDate, err := ptypes.TimestampProto(time.Now())
	if err != nil {
		return nil, err
	}

	cqrsMessage.CqrsHeader().SentDate = sentDate

	msg, err := m.delegate.Marshal(v)
	if err != nil {
		return msg, err
	}

	msg.Metadata.Set("partition", cqrsMessage.GetPartitionKey())
	return msg, nil
}

func (m PartitionedCommandEventMarshaler) Unmarshal(msg *message.Message, v interface{}) (err error) {
	err = m.delegate.Unmarshal(msg, v)
	if err != nil {
		return err
	}

	return nil
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

		bookRoomCmd := NewBookRoom(nil, fmt.Sprintf("%d", i), "John", startDate, endDate)
		if err := commandBus.Send(context.Background(), bookRoomCmd); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}
