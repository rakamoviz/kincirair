// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	fmt "fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
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

type ReservationRepository struct {
	reservations map[string]Reservation
}

func NewReservationRepository() *ReservationRepository {
	return &ReservationRepository{
		reservations: make(map[string]Reservation),
	}
}

func (repo *ReservationRepository) FindById(id string) (Reservation, error) {
	r, ok := repo.reservations[id]

	if !ok {
		keys := make([]string, len(repo.reservations))

		i := 0
		for k := range repo.reservations {
			keys[i] = k
			i++
		}

		return r, fmt.Errorf("Reservation not found for %s %v", id, keys)
	}

	return r, nil
}

func (repo *ReservationRepository) Save(r Reservation) error {
	repo.reservations[r.Id] = r

	return nil
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

type CqrsMessage interface {
	CqrsHeader() *CqrsHeader
	GetPartitionKey() string
}

func (roomBookedEvt *RoomBooked) CqrsHeader() *CqrsHeader {
	return roomBookedEvt.Header
}

func (roomBookedEvt *RoomBooked) GetPartitionKey() string {
	return roomBookedEvt.RoomId
}

func NewRoomBooked(
	trigger CqrsMessage, reservationId string, roomId string,
	guestName string, price int64,
	startDate *timestamppb.Timestamp,
	endDate *timestamppb.Timestamp,
) *RoomBooked {
	return &RoomBooked{
		Header:        NewCqrsHeader(trigger, CqrsMessageType_EVENT, Reservation{}, "", ""),
		ReservationId: reservationId,
		RoomId:        roomId,
		GuestName:     guestName,
		Price:         price,
		StartDate:     startDate,
		EndDate:       endDate,
	}
}

type BookingsFinancialReport struct {
	handledBookings     map[string]struct{}
	totalCharge         int64
	lock                sync.Mutex
	aggregateRepository *ReservationRepository
}

func NewBookingsFinancialReport(aggregateRepository *ReservationRepository) *BookingsFinancialReport {
	return &BookingsFinancialReport{
		handledBookings:     map[string]struct{}{},
		aggregateRepository: aggregateRepository,
	}
}

func (b *BookingsFinancialReport) Handle(ctx context.Context, e interface{}) error {
	// Handle may be called concurrently, so it need to be thread safe.
	b.lock.Lock()
	defer b.lock.Unlock()

	event := e.(*RoomBooked)

	fmt.Printf(
		"BookingsFinancialReport handling RoomBooked event for aggregate ID %s with partition-key %s\n",
		event.CqrsHeader().AggregateId, event.GetPartitionKey(),
	)

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
	cqrsMarshaler := PartitionedCommandEventMarshaler{cqrs.ProtobufMarshaler{}}

	reservationRepository := NewReservationRepository()
	bookingsFinancialReport := NewBookingsFinancialReport(reservationRepository)

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
