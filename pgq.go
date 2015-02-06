package pgq

import (
    "database/sql"
    "time"
)

type Event struct{
    Ev_id int64
    Ev_time time.Time
    Ev_txid int64
    Ev_retry sql.NullInt64
    Ev_type sql.NullString
    Ev_data sql.NullString
    Ev_extra1 sql.NullString
    Ev_extra2 sql.NullString
    Ev_extra3 sql.NullString
    Ev_extra4 sql.NullString
    Consumer IConsumer
}

type IConsumer interface{
    Subscribe() error
    Unsubscribe() error
    EventHandler(handler func(event Event) error)
    BatchHandler(handler func(batch []Event) error)
    Work() error
    SleepInterval(time.Duration)
    BatchId() int64
    ConsumerName() string
    SubconsumerName() string
    QueueName() string
    CloseBatch() error
}

// Register consumer in pgq
func NewConsumer(consumer_name string, queue_name string, connect_string string) (IConsumer, error) {
    consumer := new(Consumer)
    consumer.consumerName = consumer_name
    consumer.queueName = queue_name
    consumer.connectString = connect_string
    consumer.init()
    consumer.Subscribe()
    return consumer, nil
}

func NewCoopConsumer(consumer_name string, count int, queue_name string, connect_string string) (IConsumer, error) {
    coop_consumer := new(CoopConsumer)
    coop_consumer.consumerName = consumer_name
    coop_consumer.Count = count
    coop_consumer.queueName = queue_name
    coop_consumer.connectString = connect_string
    coop_consumer.init()
    return coop_consumer, nil
}

