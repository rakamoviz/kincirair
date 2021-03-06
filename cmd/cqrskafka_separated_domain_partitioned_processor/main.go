package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/mgo.v2/bson"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
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

type Aggregate interface {
	GetAggregateId() string
	GetAggregateName() string
}
type Reservation struct {
	Id        string    `bson:"_id"`
	RoomId    string    `bson:"room_id"`
	GuestName string    `bson:"guest_name"`
	Price     int64     `bson:"price"`
	StartDate time.Time `bson:"start_date"`
	EndDate   time.Time `bson:"end_date"`
}

func (r Reservation) GetAggregateId() string {
	return r.Id
}

func (r Reservation) GetAggregateName() string {
	return FullyQualifiedStructName(r)
}

type ReservationRepository struct {
	collection *mongo.Collection
}

func NewReservationRepository(collection *mongo.Collection) *ReservationRepository {
	return &ReservationRepository{
		collection,
	}
}

func (repo *ReservationRepository) FindById(ctx context.Context, id string) (Reservation, error) {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	filter := bson.M{"_id": id}
	reservation := Reservation{}

	err := repo.collection.FindOne(opCtx, filter).Decode(&reservation)

	if err == mongo.ErrNoDocuments {
		return reservation, &DBError{
			StatusCode: 0,
			Err:        err,
		}
	}

	if err != nil {
		return reservation, &DBError{
			StatusCode: -1,
			Err:        err,
		}
	}

	return reservation, nil
}

func (repo *ReservationRepository) Save(ctx context.Context, r Reservation) error {
	_, insertError := repo.collection.InsertOne(ctx, r)

	if insertError != nil {
		return &DBError{
			StatusCode: -1,
			Err:        insertError,
		}
	}

	return nil
}

type CqrsMessage interface {
	CqrsHeader() *CqrsHeader
	GetPartitionKey() string
}

func (bookRoomCmd *BookRoom) CqrsHeader() *CqrsHeader {
	return bookRoomCmd.Header
}

func (bookRoomCmd *BookRoom) GetPartitionKey() string {
	return bookRoomCmd.RoomId
}

func (roomBookedEvt *RoomBooked) CqrsHeader() *CqrsHeader {
	return roomBookedEvt.Header
}

func (roomBookedEvt *RoomBooked) GetPartitionKey() string {
	return roomBookedEvt.RoomId
}

func (orderBeerCmd *OrderBeer) CqrsHeader() *CqrsHeader {
	return orderBeerCmd.Header
}

func (orderBeerCmd *OrderBeer) GetPartitionKey() string {
	return orderBeerCmd.RoomId
}

func (beerOrderedEvt *BeerOrdered) CqrsHeader() *CqrsHeader {
	return beerOrderedEvt.Header
}

func (beerOrderedEvt *BeerOrdered) GetPartitionKey() string {
	return beerOrderedEvt.RoomId
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

func NewOrderBeer(
	trigger CqrsMessage, reservationId string,
	roomId string, count int64,
) *OrderBeer {
	return &OrderBeer{
		Header: NewCqrsHeader(
			trigger, CqrsMessageType_COMMAND,
			Reservation{Id: reservationId}, "", "",
		),
		ReservationId: reservationId,
		RoomId:        roomId,
		Count:         count,
	}
}

func NewBeerOrdered(
	trigger CqrsMessage, reservationId string,
	roomId string, count int64,
) *BeerOrdered {
	return &BeerOrdered{
		Header: NewCqrsHeader(
			trigger, CqrsMessageType_EVENT, Reservation{Id: reservationId},
			"", "",
		),
		ReservationId: reservationId,
		RoomId:        roomId,
		Count:         count,
	}
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
		cmd.Header.AggregateId, cmd.GetPartitionKey(),
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

	if err := b.eventBus.Publish(ctx,
		NewRoomBooked(
			cmd, watermill.NewUUID(), cmd.RoomId,
			cmd.GuestName, price,
			cmd.StartDate, cmd.EndDate,
		)); err != nil {
		return err
	}

	return nil
}

type RoomBookedHandler struct {
	commandBus          *cqrs.CommandBus
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
		event.Header.AggregateId, event.GetPartitionKey(),
	)

	reservation := Reservation{
		Id:        event.ReservationId,
		RoomId:    event.RoomId,
		GuestName: event.GuestName,
		Price:     event.Price,
		StartDate: event.StartDate.AsTime(),
		EndDate:   event.EndDate.AsTime(),
	}
	err := b.aggregateRepository.Save(ctx, reservation)
	if err != nil {
		return err
	}

	roomIdInt, err := strconv.Atoi(event.RoomId)
	if err != nil {
		return err
	}

	if roomIdInt%7 == 0 {
		orderBeerCmd := NewOrderBeer(
			event, event.ReservationId, event.RoomId,
			rand.Int63n(10)+1,
		)

		return b.commandBus.Send(ctx, orderBeerCmd)
	}

	return nil
}

type OrderBeerHandler struct {
	eventBus            *cqrs.EventBus
	aggregateRepository *ReservationRepository
}

func (o OrderBeerHandler) HandlerName() string {
	return "OrderBeerHandler"
}

func (o OrderBeerHandler) NewCommand() interface{} {
	return &OrderBeer{}
}

func (o OrderBeerHandler) Handle(ctx context.Context, c interface{}) error {
	cmd := c.(*OrderBeer)

	fmt.Printf(
		"OrderBeerHandler handling OrderBeer command for aggregate ID %s with partition-key %s\n",
		cmd.CqrsHeader().AggregateId, cmd.GetPartitionKey(),
	)

	if rand.Int63n(10) == 0 {
		// sometimes there is no beer left, command will be retried
		return errors.Errorf("no beer left for room %s, please try later", cmd.RoomId)
	}

	if err := o.eventBus.Publish(ctx, NewOrderBeer(
		cmd, cmd.ReservationId, cmd.RoomId,
		cmd.Count,
	)); err != nil {
		return err
	}

	log.Printf("%d beers ordered to room %s", cmd.Count, cmd.RoomId)
	return nil
}

type BeerOrderedHandler struct {
	aggregateRepository *ReservationRepository
}

func (b BeerOrderedHandler) HandlerName() string {
	return "BeerOrderedHandler"
}

// NewCommand returns type of command which this handle should handle. It must be a pointer.
func (b BeerOrderedHandler) NewEvent() interface{} {
	return &BeerOrdered{}
}

func (b BeerOrderedHandler) Handle(ctx context.Context, c interface{}) error {
	// c is always the type returned by `NewCommand`, so casting is always safe
	event := c.(*BeerOrdered)

	fmt.Printf(
		"BeerOrderedHandler handling BeerOrdered event for aggregate ID %s with partition-key %s\n",
		event.CqrsHeader(), event.GetPartitionKey(),
	)

	reservation, err := b.aggregateRepository.FindById(ctx, event.GetReservationId())
	if err != nil {
		return err
	}

	reservation.Price += 5 * event.Count

	return b.aggregateRepository.Save(ctx, reservation)
}

type PersistentCommandEventMarshaler struct {
	cqrsMessagesCollection *mongo.Collection
	delegate               cqrs.CommandEventMarshaler
}

func (m PersistentCommandEventMarshaler) Marshal(v interface{}) (*message.Message, error) {
	return m.delegate.Marshal(v)
}

func (m PersistentCommandEventMarshaler) Unmarshal(msg *message.Message, v interface{}) (err error) {
	unmarshalError := m.delegate.Unmarshal(msg, v)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if unmarshalError == nil {
		cqrsMessage, ok := v.(CqrsMessage)
		if ok {
			cqrsMessage.CqrsHeader().Name = FullyQualifiedStructName(cqrsMessage)
			_, insertError := m.cqrsMessagesCollection.InsertOne(ctx, cqrsMessage)
			if insertError != nil {
				fmt.Println("Error saving cmd ", cqrsMessage)
			}
		}
	}

	return unmarshalError
}

func (m PersistentCommandEventMarshaler) Name(v interface{}) string {
	return m.delegate.Name(v)
}

func (m PersistentCommandEventMarshaler) NameFromMessage(msg *message.Message) string {
	return m.delegate.NameFromMessage(msg)
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
	logger := watermill.NewStdLogger(false, false)

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

	// Simple middleware which will recover panics from event or command handlers.
	// More about router middlewares you can find in the documentation:
	// https://watermill.io/docs/messages-router/#middleware
	//
	// List of available middlewares you can find in message/router/middleware.
	router.AddMiddleware(middleware.Recoverer)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017/")
	client, err := mongo.Connect(ctx, clientOptions)

	time.Sleep(1 * time.Second)

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	cqrsMessagesCollection := client.Database("kincirair").Collection("cqrs_messages")
	reservationsCollection := client.Database("kincirair").Collection("reservations")

	reservationRepository := NewReservationRepository(reservationsCollection)

	cqrsMarshaler := PersistentCommandEventMarshaler{
		cqrsMessagesCollection,
		PartitionedCommandEventMarshaler{cqrs.ProtobufMarshaler{}},
	}

	// cqrs.Facade is facade for Command and Event buses and processors.
	// You can use facade, or create buses and processors manually (you can inspire with cqrs.NewFacade)
	_, err = cqrs.NewFacade(cqrs.FacadeConfig{
		GenerateCommandsTopic: func(commandName string) string {
			return commandName
		},
		CommandHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
			return []cqrs.CommandHandler{
				BookRoomHandler{eb, reservationRepository},
				OrderBeerHandler{eb, reservationRepository},
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
				RoomBookedHandler{cb, reservationRepository},
				BeerOrderedHandler{reservationRepository},
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
