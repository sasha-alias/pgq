package pgq

import (
    "log"
    "database/sql"
    "time"
    "fmt"
)

type Consumer struct{
    consumerName string
    queueName string
    connectString string
    sleepInterval time.Duration
    db *sql.DB
    current_batch_id int64
    eventHandler func(Event) error
    batchHandler func([]Event) error
}

    // Consumer name
    func (self *Consumer) ConsumerName() string {
        return self.consumerName
    }

    // Queue name
    func (self *Consumer) QueueName() string {
        return self.queueName
    }

    // Subconsumer name
    func (self *Consumer) SubconsumerName() string {
        return ""
    }

    //  Set event handler
    func (self *Consumer) EventHandler (handler func(Event) error) {
        self.eventHandler = handler
    }

    // Set batch handler
    func (self *Consumer) BatchHandler (handler func([]Event) error) {
        self.batchHandler = handler
    }


    // Set sleep interval
    func (self *Consumer) SleepInterval (interval time.Duration) {
        self.sleepInterval = interval
    }

    // Current batch id
    func (self *Consumer) BatchId () int64 {
        return self.current_batch_id
    }

    // Initialise default properties
    func (self *Consumer) init() {
        if self.sleepInterval == 0 {
            self.sleepInterval = 1*time.Second
        }
        db, err := sql.Open("postgres", self.connectString)
        if err != nil {
            log.Printf("Failed to initialize db connection %s: %v", self.connectString, err)
        }
        self.db = db
    }

    // Register consumer
    func (self *Consumer) Subscribe() error {

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

    // Unregister consumer
    func (self *Consumer) Unsubscribe() error {
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


    // Get next batch
    func (self *Consumer) nextBatch() ([]Event, error) {

        // get new batch
        var batch_id sql.NullInt64
        err := self.db.QueryRow(
            `SELECT next_batch FROM pgq.next_batch($1, $2)`,
            self.queueName, self.consumerName).Scan(&batch_id)

        if err != nil{
            ret_err := fmt.Errorf("nextBatch error when getting next batch: %v", err)
            return nil, ret_err
        }

        if !batch_id.Valid {
            self.current_batch_id = 0
            return nil, nil
        } else {
            self.current_batch_id = batch_id.Int64
        }

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
        FROM pgq.get_batch_events($1)`, self.current_batch_id)
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

    // Main working loop
    func (self *Consumer) Work () error {
        log.Printf("Consumer %s started", self.consumerName)
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
        }
        return nil
    }

    // Default batch handler
    func (self *Consumer) defaultBatchHandler(batch []Event) error {
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

    // Close current batch
    func (self *Consumer) CloseBatch() error {

        if self.current_batch_id > 0 {
            _, err := self.db.Exec("SELECT pgq.finish_batch($1)", self.current_batch_id)
            if err != nil {
                ret_err := fmt.Errorf("nextBatch error when closing batch %s: %v", self.current_batch_id, err)
                return ret_err
            }
        }
        return nil
    }

    // Default event handler
    func (self *Consumer) defaultEventHandler(event Event) error {
            log.Printf("%+v", event)
            return nil
    }

