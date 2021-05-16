// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	fmt "fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	partitions        = 1
	maxProcessingTime = 2 * time.Second
	slackTime         = 5 * time.Second
)

type BookingsFinancialReport struct {
	handledBookings map[string]struct{}
	totalCharge     int64
	lock            sync.Mutex
}

func NewBookingsFinancialReport() *BookingsFinancialReport {
	return &BookingsFinancialReport{handledBookings: map[string]struct{}{}}
}

func (b *BookingsFinancialReport) Handle(ctx context.Context, e interface{}) error {
	// Handle may be called concurrently, so it need to be thread safe.
	b.lock.Lock()
	defer b.lock.Unlock()

	event := e.(*RoomBooked)

	// When we are using Pub/Sub which doesn't provide exactly-once delivery semantics, we need to deduplicate messages.
	// GoChannel Pub/Sub provides exactly-once delivery,
	// but let's make this example ready for other Pub/Sub implementations.
	if _, ok := b.handledBookings[event.ReservationId]; ok {
		return nil
	}
	b.handledBookings[event.ReservationId] = struct{}{}

	b.totalCharge += event.Price

	fmt.Printf(">>> Already booked rooms for $%d\n", b.totalCharge)
	return nil
}

func main() {
	saramaSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaSubscriberConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               []string{"localhost:9092"},
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: configsByProcessingTime(saramaSubscriberConfig),
			ConsumerGroup:         "bookings_financial_report",
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "events")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	process(ctx, messages)
}

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func processMessage(
	ctx context.Context, cqrsMarshaler cqrs.CommandEventMarshaler,
	expectedEventNames []string, msg *message.Message,
	bookingsFinancialReport *BookingsFinancialReport,
) {
	defer msg.Ack()

	messageEventName := cqrsMarshaler.NameFromMessage(msg)
	_, messageExpected := Find(expectedEventNames, messageEventName)
	if !messageExpected {
		return
	}

	var event interface{}
	if messageEventName == "main.RoomBooked" {
		event = &RoomBooked{}
	}

	cqrsMarshaler.Unmarshal(msg, event)

	bookingsFinancialReport.Handle(ctx, event)
}

func process(ctx context.Context, messages <-chan *message.Message) {
	cqrsMarshaler := cqrs.ProtobufMarshaler{}
	bookingsFinancialReport := NewBookingsFinancialReport()

	expectedEventNames := []string{}
	expectedEvents := []interface{}{&RoomBooked{}}
	for _, expectedEvent := range expectedEvents {
		expectedEventNames = append(expectedEventNames, cqrsMarshaler.Name(expectedEvent))
	}

	for msg := range messages {
		processMessage(
			ctx, cqrsMarshaler,
			expectedEventNames, msg,
			bookingsFinancialReport,
		)
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
