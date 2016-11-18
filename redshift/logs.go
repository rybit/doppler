package redshift

import (
	"database/sql"

	"strings"

	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/pkg/errors"

	"reflect"

	"time"

	"github.com/rybit/doppler/messaging"
)

const (
	createLines = `
	create table if not exists lines (
		id varchar(40) primary key,
		msg varchar(max) not null,
		timestamp timestamptz not null,
		source varchar(max) not null,
		level varchar(10) not null,
		hostname varchar(max),

		created_at timestamptz default current_timestamp
	)
	sortkey (timestamp)
	`

	createLinesDims = `
	create table if not exists line_dims (
		id bigint identity primary key,
		key varchar(max) not null,
		value varchar(max) not null,
		line_id varchar(40) references lines(id)
	)
	sortkey (line_id)
	`

	insertLine    = `insert into lines (id, source, msg, timestamp, level, hostname, created_at) values`
	insertLineDim = `insert into line_dims (key, value, line_id) values `

	MaxLineLength = 65536
)

func ProcessLines(nc *nats.Conn, log *logrus.Entry, config *Config) error {
	log.WithFields(logrus.Fields{
		"db":   config.DB,
		"host": config.Host,
		"port": config.Port,
	}).Info("Connecting to Redshift")
	db, err := ConnectToRedshift(
		config.Host,
		config.Port,
		config.DB,
		config.User,
		config.Pass,
		config.Timeout,
	)
	if err != nil {
		return err
	}

	if err := createLogsTables(db); err != nil {
		return err
	}

	handler := buildLogsHandler(db, config.LogQueries)
	sub, wg, err := messaging.ConsumeInBatches(nc, log, config.MetricsConf, handler)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	wg.Wait()
	return nil
}

func createLogsTables(db *sql.DB) error {
	if _, err := db.Exec(createLines); err != nil {
		return errors.Wrap(err, "creating metrics table")
	}

	if _, err := db.Exec(createLinesDims); err != nil {
		return errors.Wrap(err, "creating dims table")
	}

	return nil
}

func buildLogsHandler(db *sql.DB, verbose bool) messaging.BatchHandler {
	return func(batch map[time.Time]*nats.Msg, log *logrus.Entry) {
		lineValues := []string{}
		dimValues := []string{}
		for _, raw := range batch {
			log := log.WithField("source", raw.Subject)

			msg, err := messaging.ExtractLogMsg(raw.Data, log)
			if err != nil {
				log.WithError(err).Warn("Failed to parse log line - skipping it")
				continue
			}

			// validate the message
			if len(msg.Msg) > MaxLineLength {
				log.Debugf("Skipping line because it is too long: '%s'", msg.Msg)
				continue
			}

			msg.Msg = sanitize(msg.Msg)

			// convert the dims to strings
			for k, v := range msg.Dims {
				if asStr, ok := asString(v); ok {
					dimValues = append(dimValues, fmt.Sprintf("('%s', '%s', '%s')", k, asStr, id))
				} else {
					log.Warnf("Unsupported type for dim: %s, type: %s, value: %v", k, reflect.TypeOf(v).String(), v)
				}
			}

			lineValues = append(lineValues, fmt.Sprintf(
				"('%s', '%s', '%s', '%s', '%s', '%s', default)",
				id,
				raw.Subject,
				msg.Msg,
				msg.Timestamp.Format(time.RFC822Z),
				msg.Level,
				msg.Hostname,
			))
		}

		log.WithFields(logrus.Fields{
			"batch_size":  len(batch),
			"lines_count": len(lineValues),
			"dims_count":  len(dimValues),
		}).Info("Finished parsing batch - Storing batch")

		tx, err := db.Begin()
		if err != nil {
			log.WithError(err).Warn("Failed to create transaction")
			return
		}

		if err := insert(tx, insertLine, lineValues, verbose, log.WithField("phase", "lines")); err != nil {
			tx.Rollback()
			return
		}

		if err := insert(tx, insertLineDim, dimValues, verbose, log.WithField("phase", "dims")); err != nil {
			tx.Rollback()
			return
		}

		if err := tx.Commit(); err != nil {
			log.WithError(err).Warn("Failed to commit transaction")
		}
	}
}

func sanitize(in string) string {
	return strings.Replace(in, "'", "", -1)
}

func asString(face interface{}) (string, bool) {
	switch face.(type) {
	case int, int32, int64:
		return fmt.Sprintf("%d", face), true
	case string:
		return face.(string), true
	case float32, float64:
		return fmt.Sprintf("%f", face), true
	case bool:
		return fmt.Sprintf("%t", face), true
	}
	return "", false
}
