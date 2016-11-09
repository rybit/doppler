package redshift

import (
	"database/sql"

	"encoding/json"

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

func CreateLogsTables(db *sql.DB) error {
	if _, err := db.Exec(createLines); err != nil {
		return errors.Wrap(err, "creating metrics table")
	}

	if _, err := db.Exec(createLinesDims); err != nil {
		return errors.Wrap(err, "creating dims table")
	}

	return nil
}

func BuildLogsHandler(db *sql.DB, verbose bool) messaging.BatchHandler {
	return func(batch []*nats.Msg, log *logrus.Entry) {
		lineValues := []string{}
		dimValues := []string{}
		for _, raw := range batch {
			log := log.WithField("source", raw.Subject)
			m := map[string]interface{}{}
			if err := json.Unmarshal(raw.Data, &m); err == nil {
				id := id()

				level := "unknown"
				var msg, timestamp, hostname string
				for k, v := range m {
					k = strings.ToLower(k)
					switch k {
					case "msg", "@msg":
						if val, ok := v.(string); ok {
							val = strings.TrimSpace(val)
							val = sanitize(val)
							if len(val) > MaxLineLength {
								log.Debugf("Skipping line because it is too long: '%s'", val)
								continue
							}
							if len(val) == 0 {
								continue
							}
							msg = val
						}
					case "time", "@timestamp", "@time", "timestamp", "_timestamp":
						if val, ok := v.(string); ok {
							timestamp = val
						}
					case "level":
						if val, ok := v.(string); ok {
							level = val
						}
					case "hostname", "host":
						if val, ok := v.(string); ok {
							hostname = val
						}
					default:
						if asStr, ok := asString(v); ok {
							dimValues = append(dimValues, fmt.Sprintf("('%s', '%s', '%s')", k, asStr, id))
						} else {
							log.Warnf("Unsupported type for dim: %s, type: %s, value: %v", k, reflect.TypeOf(v).String(), v)
						}
					}
				}
				if msg != "" {
					if timestamp == "" {
						timestamp = time.Now().Format(time.RFC822Z)
						log.Debugf("Failed to find a timestamp field, using now. source %s, msg: %s", raw.Subject, string(raw.Data))
					}
					lineValues = append(lineValues, fmt.Sprintf(
						"('%s', '%s', '%s', '%s', '%s', '%s', default)",
						id,
						raw.Subject,
						msg,
						timestamp,
						level,
						hostname,
					))
				} else {
					log.Warnf("failed to find 'msg' from '%s' field in %s", raw.Subject, string(raw.Data))
				}
			} else {
				log.WithError(err).Warn("Failed to parse incoming message - skipping")
			}
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
