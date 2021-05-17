// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	fmt "fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/mgo.v2/bson"
)

const (
	partitions        = 2
	maxProcessingTime = 2 * time.Second
	slackTime         = 5 * time.Second
)

type DBError struct {
	StatusCode int

	Err error
}

func (r *DBError) Error() string {
	return r.Err.Error()
}

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

type FinancialBooking struct {
	Id    string `bson:"_id"`
	Total int64  `bson:"total"`
}

type FinancialBookingRepository struct {
	collection *mongo.Collection
}

func NewFinancialBookingRepository(collection *mongo.Collection) *FinancialBookingRepository {
	return &FinancialBookingRepository{
		collection: collection,
	}
}

func (repo *FinancialBookingRepository) FindById(ctx context.Context, id string) (FinancialBooking, error) {
	opCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	filter := bson.M{"_id": id}
	financialBooking := FinancialBooking{}

	err := repo.collection.FindOne(opCtx, filter).Decode(&financialBooking)

	if err == mongo.ErrNoDocuments {
		return financialBooking, &DBError{
			StatusCode: 0,
			Err:        err,
		}
	}

	if err != nil {
		return financialBooking, &DBError{
			StatusCode: -1,
			Err:        err,
		}
	}

	return financialBooking, nil
}

func (repo *FinancialBookingRepository) Save(ctx context.Context, f FinancialBooking) error {
	opCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	filter := bson.M{"_id": f.Id}
	update := bson.M{"$set": f}
	upsert := true

	_, upsertError := repo.collection.UpdateOne(opCtx, filter, update, &options.UpdateOptions{
		Upsert: &upsert,
	})

	if upsertError != nil {
		return &DBError{
			StatusCode: -1,
			Err:        upsertError,
		}
	}

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
		AggregateId:   aggregate.GetAggregateId(),
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
		Header: NewCqrsHeader(
			trigger, CqrsMessageType_EVENT, Reservation{Id: reservationId},
			"", "",
		),
		ReservationId: reservationId,
		RoomId:        roomId,
		GuestName:     guestName,
		Price:         price,
		StartDate:     startDate,
		EndDate:       endDate,
	}
}

type FinancialBookingBuilder struct {
	id         string
	lock       sync.Mutex
	repository *FinancialBookingRepository
}

func NewFinancialBookingBuilder(id string, repository *FinancialBookingRepository) *FinancialBookingBuilder {
	return &FinancialBookingBuilder{
		id:         id,
		repository: repository,
	}
}

func (b *FinancialBookingBuilder) Handle(ctx context.Context, e interface{}) error {
	// Handle may be called concurrently, so it need to be thread safe.
	b.lock.Lock()
	defer b.lock.Unlock()

	event := e.(*RoomBooked)

	fmt.Printf(
		"BookingsFinancialReport handling RoomBooked event for aggregate ID %s with partition-key %s\n",
		event.CqrsHeader().AggregateId, event.GetPartitionKey(),
	)

	financialBooking := FinancialBooking{Id: b.id, Total: 0}
	fb, err := b.repository.FindById(ctx, b.id)

	if err != nil {
		dbError, ok := err.(*DBError)
		if !ok {
			return err
		}

		if dbError.StatusCode != 0 {
			return err
		}

		financialBooking.Total = event.Price
	} else {
		financialBooking.Total = fb.Total + event.Price
	}

	err = b.repository.Save(ctx, financialBooking)
	if err != nil {
		return err
	}

	fmt.Printf(">>> Current financial booking $%v\n", financialBooking)
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
	financialBookingBuilder *FinancialBookingBuilder,
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

	financialBookingBuilder.Handle(ctx, event)
}

func process(ctx context.Context, messages <-chan *message.Message) {
	cqrsMarshaler := PartitionedCommandEventMarshaler{cqrs.ProtobufMarshaler{}}

	ctx = context.Background()
	opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017/")
	client, err := mongo.Connect(opCtx, clientOptions)

	time.Sleep(1 * time.Second)

	err = client.Ping(opCtx, nil)
	if err != nil {
		log.Fatal(err)
	}

	financialBookingsCollection := client.Database("kincirair").Collection("financial_bookings")

	financialBookingRepository := NewFinancialBookingRepository(financialBookingsCollection)
	financialBookingBuilder := NewFinancialBookingBuilder("voc", financialBookingRepository)

	expectedEventNames := []string{}
	expectedEvents := []interface{}{&RoomBooked{}}
	for _, expectedEvent := range expectedEvents {
		expectedEventNames = append(expectedEventNames, cqrsMarshaler.Name(expectedEvent))
	}

	for msg := range messages {
		processMessage(
			ctx, cqrsMarshaler,
			expectedEventNames, msg,
			financialBookingBuilder,
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
