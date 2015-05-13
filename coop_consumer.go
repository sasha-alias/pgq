// Package pgq provides an easy framework for writing consumers for PGQ.
// PGQ is an implementation of transactional queues on top of Postgresql.
package pgq

import (
    "log"
    "database/sql"
    "time"
    "fmt"
    "strconv"
    _ "github.com/lib/pq"
)


// Cooperative consumer
type CoopConsumer struct {
    consumerName string
    Count int
    queueName string
    connectString string
    subConsumers map[string]*Subconsumer
    sleepInterval time.Duration
    db *sql.DB
    eventHandler func(Event) error
    batchHandler func([]Event) error
}

    // Consumer name
    func (self *CoopConsumer) ConsumerName() string {
        return self.consumerName
    }

    // Queue name
    func (self *CoopConsumer) QueueName() string {
        return self.queueName
    }

    // Subconsumer name
    func (self *CoopConsumer) SubconsumerName() string {
        return ""
    }

    // Consumer Id
    func (self *CoopConsumer) ConsumerId() string {
        return self.consumerName
    }

    //  Set event handler
    func (self *CoopConsumer) EventHandler (handler func(Event) error) {
        self.eventHandler = handler
        for _, subconsumer := range self.subConsumers{
            subconsumer.EventHandler(handler)
        }
    }

    // Set batch handler
    func (self *CoopConsumer) BatchHandler (handler func([]Event) error) {
        self.batchHandler = handler
        for _, subconsumer := range self.subConsumers{
            subconsumer.BatchHandler(handler)
        }
    }

    // Current batch id
    func (self *CoopConsumer) BatchId () int64{
        return 0  // coop consumer doesn't have batches, only subconsumers do
    }

    // Set sleep interval
    func (self *CoopConsumer) SleepInterval(interval time.Duration){
        self.sleepInterval = interval
        for _, subconsumer := range self.subConsumers{
            subconsumer.SleepInterval(interval)
        }
    }

    // Register consumer on the queue
    func (self *CoopConsumer) Subscribe() error {
        var register_consumer int32
        err := self.db.QueryRow(
            "SELECT register_consumer FROM pgq.register_consumer($1, $2)",
            self.queueName, self.consumerName).Scan(&register_consumer)
        if err != nil {
            err := fmt.Errorf("Failed to register consumer %s on %s: %s", self.consumerName, self.queueName, err)
            log.Println(err)
            return err
        }
        if register_consumer == 1 {
            log.Printf("Consumer %s registered on %s", self.consumerName, self.queueName)
        } else if register_consumer == 0 {
            log.Printf("Consumer %s on %s aready registered", self.consumerName, self.queueName)
        }
        return nil
    }

    // Unregister consumer from the queue
    func (self *CoopConsumer) Unsubscribe() error {
        var unregister_consumer int32
        err := self.db.QueryRow(
            "SELECT unregister_consumer FROM pgq.unregister_consumer($1, $2)",
            self.queueName, self.consumerName).Scan(&unregister_consumer)
        if err != nil {
            err := fmt.Errorf("Failed to unregister consumer %s from %s: %s", self.consumerName, self.queueName, err)
            log.Println(err)
            return err
        }
        log.Printf("%v of (sub)consumers %s unregistered from %s", unregister_consumer, self.consumerName, self.queueName)
        return nil
    }

    // Initialize default properties
    func (self *CoopConsumer) init() {
        if self.sleepInterval == 0 {
            self.sleepInterval = 1*time.Second
        }
        db, err := sql.Open("postgres", self.connectString)
        if err != nil {
            log.Printf("Failed to initialize db connection %s: %v", self.connectString, err)
        }
        self.db = db

        self.subConsumers = make(map[string]*Subconsumer)
        for i := 0; i < self.Count; i++ {
            subconsumer := new(Subconsumer)
            subconsumer.consumerName = self.consumerName
            subconsumer.subconsumerName = strconv.Itoa(i)
            subconsumer.queueName = self.queueName
            subconsumer.db = self.db
            subconsumer.eventHandler = self.eventHandler
            subconsumer.batchHandler = self.batchHandler
            self.subConsumers[subconsumer.subconsumerName] = subconsumer
        }
    }

    // Main working loop
    func (self *CoopConsumer) Work() error {
        for _, subconsumer := range self.subConsumers {
            go subconsumer.Work()
        }
        wait := make(chan int)
        <-wait
        return nil
    }

    func (self *CoopConsumer) CloseBatch() error {
        return fmt.Errorf("Coop consumer doesn't have batches, only subconsumers do")
    }

// Subconsumer
type Subconsumer struct{
    consumerName string
    subconsumerName string
    queueName string
    sleepInterval time.Duration
    db *sql.DB
    current_batch_id int64
    eventHandler func(Event) error
    batchHandler func([]Event) error
}

    // Consumer name
    func (self *Subconsumer) ConsumerName() string {
        return self.consumerName
    }

    // Subconsumer name
    func (self *Subconsumer) SubconsumerName() string {
        return self.subconsumerName
    }

    // Queue name
    func (self *Subconsumer) QueueName() string {
        return self.queueName
    }

    //  Set event handler
    func (self *Subconsumer) EventHandler (handler func(Event) error) {
        self.eventHandler = handler
    }

    // Set batch handler
    func (self *Subconsumer) BatchHandler (handler func([]Event) error) {
        self.batchHandler = handler
    }


    // Set sleep interval
    func (self *Subconsumer) SleepInterval (interval time.Duration) {
        self.sleepInterval = interval
    }


    // Register subconsumer
    func (self *Subconsumer) Subscribe() error {

        var register_consumer int32
        err := self.db.QueryRow(
            "SELECT register_consumer FROM pgq_coop.register_subconsumer($1, $2, $3)",
            self.queueName, self.consumerName, self.subconsumerName).Scan(&register_consumer)
        if err != nil {
            err := fmt.Errorf("Failed to register subconsumer %s.%s on %s: %s", self.consumerName, self.subconsumerName, self.queueName, err)
            log.Println(err)
            return err
        }
        if register_consumer == 1 {
            log.Printf("Subonsumer %s.%s registered on %s", self.consumerName, self.subconsumerName, self.queueName)
        } else if register_consumer == 0 {
            log.Printf("Subconsumer %s.%s on %s aready registered", self.consumerName, self.subconsumerName, self.queueName)
        }
        return nil
    }


    // Unregister subconsumer
    func (self *Subconsumer) Unsubscribe() error {
        var unregister_subconsumer int32
        err := self.db.QueryRow(
            "SELECT unregister_subconsumer FROM pgq.unregister_subconsumer($1, $2, $3, 0)",
            self.queueName, self.consumerName, self.subconsumerName).Scan(&unregister_subconsumer)
        if err != nil {
            err := fmt.Errorf("Failed to unregister subconsumer %s.%s from %s: %s", self.consumerName, self.subconsumerName, self.queueName, err)
            log.Println(err)
            return err
        }
        if unregister_subconsumer == 0 {
            log.Printf("Subconsumer %s.%s not found", self.consumerName, self.subconsumerName)
        } else if unregister_subconsumer == 1 {
            log.Printf("Subconsumer %s.%s unregistered from %s", unregister_subconsumer, self.consumerName, self.subconsumerName, self.queueName)
        }
        return nil
    }

    // Working loop running in goroutine
    func (self *Subconsumer) Work() error {
        log.Printf("Subconsumer %s.%s started", self.consumerName, self.subconsumerName)
        for {
            batch, err := self.nextBatch()
            if err != nil {
                log.Println(err)
                return err
            }

            if batch == nil {
                time.Sleep(self.sleepInterval)
            } else {

                var err error
                if self.batchHandler == nil {
                    err = self.defaultBatchHandler(batch)
                } else {
                    err = self.batchHandler(batch)
                }

                if err != nil {
                    log.Println(err)
                    return err
                }
            }

            close_err := self.CloseBatch()
            if close_err != nil {
                return close_err
            }
            time.Sleep(self.sleepInterval)
        }
        return nil
    }


    // Get next batch
    func (self *Subconsumer) nextBatch() ([]Event, error) {

        // get new batch
        var batch_id sql.NullInt64
        err := self.db.QueryRow(
            `SELECT next_batch FROM pgq_coop.next_batch($1, $2, $3)`,
            self.queueName, self.consumerName, self.subconsumerName).Scan(&batch_id)
        if err != nil{
            ret_err := fmt.Errorf("nextBatch error when getting next batch: %v", err)
            return nil, ret_err
        }

        if !batch_id.Valid {
            return nil, nil
        }
        self.current_batch_id = batch_id.Int64

        // get batch events
        rows, ev_err := self.db.Query(
        `SELECT
            ev_id,
            ev_time,
            ev_txid,
            ev_retry,
            ev_type,
            ev_data,
            ev_extra1,
            ev_extra2,
            ev_extra3,
            ev_extra4
        FROM pgq.get_batch_events($1)`, batch_id.Int64)
        defer rows.Close()

        if ev_err != nil {
            ret_err := fmt.Errorf("nextBatch error when getting events from batch %s: %v", self.current_batch_id, ev_err)
            return nil, ret_err
        }

        batch := []Event{}

        for rows.Next() {
            var event Event
            rows_err := rows.Scan(
                &event.Ev_id,
                &event.Ev_time,
                &event.Ev_txid,
                &event.Ev_retry,
                &event.Ev_type,
                &event.Ev_data,
                &event.Ev_extra1,
                &event.Ev_extra2,
                &event.Ev_extra3,
                &event.Ev_extra4)
            //event.BatchInfo = &self.current_batch_info
            event.Consumer = self
            batch = append(batch, event)
            if rows_err != nil {
                ret_err := fmt.Errorf("nextBatch error during row processing: %v", rows_err)
                return nil, ret_err
            }
        }
        rows_err := rows.Err()
        if rows_err != nil {
            ret_err := fmt.Errorf("nextBatch error during rows processing: %v", rows_err)
            return nil, ret_err
        }

        return batch, nil

    }

    // Close current batch
    func (self *Subconsumer) CloseBatch() error {

        if self.current_batch_id != 0 {
            _, err := self.db.Exec("SELECT pgq_coop.finish_batch($1)", self.current_batch_id)
            if err != nil {
                ret_err := fmt.Errorf("nextBatch error when closing batch %s: %v", self.current_batch_id, err)
                return ret_err
            }
        }
        return nil
    }

    // Get current batch id
    func (self *Subconsumer) BatchId() int64 {
        return self.current_batch_id
    }

    // Default batch handler
    func (self *Subconsumer) defaultBatchHandler(batch []Event) error {
        for _, event := range batch{
            var err error
            if self.eventHandler == nil {
                err = self.defaultEventHandler(event)
            } else {
                err =self.eventHandler(event)
            }
            if err != nil {
                return err
            }
        }
        return nil
    }

    // Default event handler
    func (self *Subconsumer) defaultEventHandler(event Event) error {
            log.Printf("%+v", event)
            return nil
    }

