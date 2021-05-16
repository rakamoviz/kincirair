package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/Shopify/sarama"
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
		return r, fmt.Errorf("Reservation not found for %s", id)
	}

	return r, nil
}

func (repo *ReservationRepository) Save(r Reservation) error {
	repo.reservations[r.Id] = r

	return nil
}

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

func (roomBookedEvt *RoomBooked) GetCqrsAggregateId() string {
	return roomBookedEvt.RoomId
}

func (roomBookedEvt *RoomBooked) GetCqrsAggregateName() string {
	return roomBookedEvt.RoomId
}

func (roomBookedEvt *RoomBooked) GetPartitionKey() string {
	return roomBookedEvt.RoomId
}

// BookRoomHandler is a command handler, which handles BookRoom command and emits RoomBooked.
//
// In CQRS, one command must be handled by only one handler.
// When another handler with this command is added to command processor, error will be retuerned.
type BookRoomHandler struct {
	eventBus            *cqrs.EventBus
	aggregateRepository *ReservationRepository
}

func (b BookRoomHandler) HandlerName() string {
	return "BookRoomHandler"
}

// NewCommand returns type of command which this handle should handle. It must be a pointer.
func (b BookRoomHandler) NewCommand() interface{} {
	return &BookRoom{}
}

func (b BookRoomHandler) Handle(ctx context.Context, c interface{}) error {
	// c is always the type returned by `NewCommand`, so casting is always safe
	cmd := c.(*BookRoom)

	fmt.Printf(
		"BookRoomHandler handling BookRoom command for aggregate ID %s with partition-key %s\n",
		cmd.GetCqrsAggregateId(), cmd.GetPartitionKey(),
	)

	// some random price, in production you probably will calculate in wiser way
	price := (rand.Int63n(40) + 1) * 10

	log.Printf(
		"Booked %s for %s from %s to %s",
		cmd.RoomId,
		cmd.GuestName,
		time.Unix(cmd.StartDate.Seconds, int64(cmd.StartDate.Nanos)),
		time.Unix(cmd.EndDate.Seconds, int64(cmd.EndDate.Nanos)),
	)

	if err := b.eventBus.Publish(ctx, &RoomBooked{
		ReservationId: watermill.NewUUID(),
		RoomId:        cmd.RoomId,
		GuestName:     cmd.GuestName,
		Price:         price,
		StartDate:     cmd.StartDate,
		EndDate:       cmd.EndDate,
	}); err != nil {
		return err
	}

	return nil
}

type RoomBookedHandler struct {
	aggregateRepository *ReservationRepository
}

func (b RoomBookedHandler) HandlerName() string {
	return "RoomBookedHandler"
}

// NewCommand returns type of command which this handle should handle. It must be a pointer.
func (b RoomBookedHandler) NewEvent() interface{} {
	return &RoomBooked{}
}

func (b RoomBookedHandler) Handle(ctx context.Context, c interface{}) error {
	// c is always the type returned by `NewCommand`, so casting is always safe
	event := c.(*RoomBooked)

	fmt.Printf(
		"RoomBookedHandler handling RoomBooked event for aggregate ID %s with partition-key %s\n",
		event.GetCqrsAggregateId(), event.GetPartitionKey(),
	)

	reservation := Reservation{
		Id:        event.ReservationId,
		RoomId:    event.RoomId,
		GuestName: event.GuestName,
		Price:     event.Price,
		StartDate: event.StartDate.AsTime(),
		EndDate:   event.EndDate.AsTime(),
	}

	return b.aggregateRepository.Save(reservation)
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
	logger := watermill.NewStdLogger(false, false)
	cqrsMarshaler := PartitionedCommandEventMarshaler{cqrs.ProtobufMarshaler{}}

	commandsSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
	commandsSubscriberConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	commandsSubscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               []string{"localhost:9092"},
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: configsByProcessingTime(commandsSubscriberConfig),
			ConsumerGroup:         "commands_subscriber",
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

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

	eventsPublisherConfig := kafka.DefaultSaramaSyncPublisherConfig()
	eventsPublisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers: []string{"localhost:9092"},
			Marshaler: kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
				return msg.Metadata.Get("partition"), nil
			}),
			OverwriteSaramaConfig: eventsPublisherConfig,
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	// CQRS is built on messages router. Detailed documentation: https://watermill.io/docs/messages-router/
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	reservationRepository := NewReservationRepository()

	// Simple middleware which will recover panics from event or command handlers.
	// More about router middlewares you can find in the documentation:
	// https://watermill.io/docs/messages-router/#middleware
	//
	// List of available middlewares you can find in message/router/middleware.
	router.AddMiddleware(middleware.Recoverer)

	// cqrs.Facade is facade for Command and Event buses and processors.
	// You can use facade, or create buses and processors manually (you can inspire with cqrs.NewFacade)
	_, err = cqrs.NewFacade(cqrs.FacadeConfig{
		GenerateCommandsTopic: func(commandName string) string {
			return commandName
		},
		CommandHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
			return []cqrs.CommandHandler{
				BookRoomHandler{eb, reservationRepository},
			}
		},
		CommandsPublisher: commandsPublisher,
		CommandsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			// we can reuse subscriber, because all commands have separated topics
			return commandsSubscriber, nil
		},
		GenerateEventsTopic: func(eventName string) string {
			return "events"
		},
		EventHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.EventHandler {
			return []cqrs.EventHandler{
				RoomBookedHandler{reservationRepository},
			}
		},
		EventsPublisher: eventsPublisher,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			eventsSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
			eventsSubscriberConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

			return kafka.NewSubscriber(
				kafka.SubscriberConfig{
					Brokers:               []string{"localhost:9092"},
					Unmarshaler:           kafka.DefaultMarshaler{},
					OverwriteSaramaConfig: configsByProcessingTime(eventsSubscriberConfig),
					ConsumerGroup:         handlerName,
				},
				watermill.NewStdLogger(false, false),
			)
		},
		Router:                router,
		CommandEventMarshaler: cqrsMarshaler,
		Logger:                logger,
	})
	if err != nil {
		panic(err)
	}

	// processors are based on router, so they will work when router will start
	if err := router.Run(context.Background()); err != nil {
		panic(err)
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
