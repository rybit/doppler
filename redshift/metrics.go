package redshift

import (
	"database/sql"

	"encoding/json"
	"time"

	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/pkg/errors"

	"reflect"

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

	createMetricDims = `
	create table if not exists metric_dims (
		id bigint identity primary key,
		key varchar(max) not null,
		value varchar(max) not null,
		metric_id varchar(40) references metrics(id)
	)
	sortkey (metric_id)
	`
	insertMetric    = `insert into metrics (id, name, timestamp, value, type, created_at) values `
	insertMetricDim = `insert into metric_dims (key, value, metric_id) values `
)

func CreateMetricsTables(db *sql.DB) error {
	if _, err := db.Exec(createMetrics); err != nil {
		return errors.Wrap(err, "creating metrics table")
	}

	if _, err := db.Exec(createMetricDims); err != nil {
		return errors.Wrap(err, "creating dims table")
	}

	return nil
}

func BuildMetricsHandler(db *sql.DB, verbose bool) messaging.BatchHandler {
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
						if asStr, ok := asString(v); ok {
							dimValues = append(dimValues, fmt.Sprintf("('%s', '%s', '%s')", k, asStr, id))
						} else {
							log.Warnf("Unsupported type for dim: %s, type: %s, value: %v", k, reflect.TypeOf(v).String(), v)
						}
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

		if err := insert(tx, insertMetric, metricValues, verbose, log.WithField("phase", "metrics")); err != nil {
			tx.Rollback()
			return
		}

		if err := insert(tx, insertMetricDim, dimValues, verbose, log.WithField("phase", "dims")); err != nil {
			tx.Rollback()
			return
		}

		if err := tx.Commit(); err != nil {
			log.WithError(err).Warn("Failed to commit transaction")
		}
	}
}
