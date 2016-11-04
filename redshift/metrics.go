package redshift

import (
	"database/sql"

	"encoding/json"
	"time"

	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/pkg/errors"

	"github.com/rybit/doppler/messaging"
)

const (
	createMetrics = `
	create table if not exists metrics (
		id varchar(40) primary key,
		name varchar(max) not null,
		type varchar(50) not null,
		timestamp timestamptz not null,
		value bigint not null,

		created_at timestamptz default current_timestamp
	)
	compound sortkey (name, timestamp)
	`

	createDims = `
	create table if not exists dims (
		id bigint identity primary key,
		key varchar(max) not null,
		value varchar(max) not null,
		metric_id varchar(40) references metrics(id)
	)
	sortkey (metric_id)
	`
	metricInsert = `insert into metrics (id, name, timestamp, value, type, created_at) values `
	dimInsert    = `insert into dims (key, value, metric_id) values `
)

func createMetricsTables(db *sql.DB) error {
	if _, err := db.Exec(createMetrics); err != nil {
		return errors.Wrap(err, "creating metrics table")
	}

	if _, err := db.Exec(createDims); err != nil {
		return errors.Wrap(err, "creating dims table")
	}

	return nil
}

func ProcessMetrics(host string, port int, mconf *IngestionConfig, nc *nats.Conn, log *logrus.Entry) error {
	log.WithFields(logrus.Fields{
		"db":   mconf.DB,
		"host": host,
		"port": port,
	}).Info("Connecting to Redshift")
	db, err := connectToRedshift(
		host,
		port,
		mconf.DB,
		mconf.User,
		mconf.Pass,
		mconf.Timeout,
	)
	if err != nil {
		return err
	}

	if err := createMetricsTables(db); err != nil {
		return err
	}

	// build worker pool
	shared := make(chan *nats.Msg, mconf.BufferSize)
	bufSub := &messaging.BufferedSubscriber{
		Subject:  mconf.Subject,
		Group:    mconf.Group,
		Messages: shared,
	}
	wg, err := messaging.BuildBatchingWorkerPool(shared, mconf.PoolSize, mconf.BatchSize, mconf.BatchTimeout, log, getMetricBatchHandler(db))
	if err != nil {
		return err
	}

	if err := bufSub.Subscribe(nc, log); err != nil {
		return err
	}
	defer bufSub.Unsubscribe()

	wg.Wait()
	return nil
}

func getMetricBatchHandler(db *sql.DB) messaging.BatchHandler {
	return func(batch []*nats.Msg, log *logrus.Entry) {
		metricValues := []string{}
		dimValues := []string{}
		for _, raw := range batch {
			m := new(messaging.InboundMetric)
			if err := json.Unmarshal(raw.Data, m); err == nil {
				id := id()
				if m.Timestamp.IsZero() {
					m.Timestamp = time.Now()
				}

				tail := fmt.Sprintf(
					"('%s', '%s', '%s', %d, '%s', default)",
					id,
					m.Name,
					m.Timestamp.Format(time.RFC822Z),
					m.Value,
					m.Type,
				)
				metricValues = append(metricValues, tail)
				if m.Dims != nil {
					for k, v := range *m.Dims {
						asStr := ""
						switch t := v.(type) {
						case int, int32, int64:
							asStr = fmt.Sprintf("%d", v)
						case string:
							asStr = v.(string)
						case float32, float64:
							asStr = fmt.Sprintf("%f", v)
						case bool:
							asStr = fmt.Sprintf("%t", v)
						default:
							log.Warnf("Unsupported type for dim: %s, type: %v, value: %v", k, t, v)
							continue
						}
						dimValues = append(dimValues, fmt.Sprintf("('%s', '%s', '%s')", k, asStr, id))
					}
				}
			} else {
				log.WithError(err).Warn("Failed to parse incoming message - skipping")
			}
		}

		log.WithFields(logrus.Fields{
			"batch_size":    len(batch),
			"metrics_count": len(metricValues),
			"dims_count":    len(dimValues),
		}).Info("Finished parsing batch - Storing batch")

		tx, err := db.Begin()
		if err != nil {
			log.WithError(err).Warn("Failed to create transaction")
			return
		}

		if err := insert(tx, metricInsert, metricValues, log.WithField("phase", "metrics")); err != nil {
			tx.Rollback()
			return
		}

		if err := insert(tx, dimInsert, dimValues, log.WithField("phase", "dims")); err != nil {
			tx.Rollback()
			return
		}

		if err := tx.Commit(); err != nil {
			log.WithError(err).Warn("Failed to commit transaction")
		}
	}
}
