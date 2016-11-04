package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	_ "github.com/lib/pq" // Postgres driver.
	"github.com/pkg/errors"

	"sync"

	"sync/atomic"

	"github.com/pborman/uuid"
	"github.com/rybit/doppler/messaging"
)

const (
	metricInsert = `insert into metrics (id, name, timestamp, value, type, created_at) values `

	// this is meant to be all but the necessary '(?, ?, ?)'
	dimInsert = `INSERT into dims (key, value, metric_id) VALUES `
)

type RedshiftConfig struct {
	Host    string  `mapstructure:"host"`
	Port    int     `mapstructure:"port"`
	DB      string  `mapstructure:"db"`
	User    *string `mapstructure:"user"`
	Pass    *string `mapstructure:"pass"`
	Timeout int     `mapstructure:"connect_timeout"`

	BatchTimeout int `mapstructure:"batch_timeout"`
	BatchSize    int `mapstructure:"batch_size"`

	LogQueries bool `mapstructure:"log_queries"`
}

func id() string {
	return uuid.NewRandom().String()
}

var LogQueries = false

func ConnectToRedshift(config *RedshiftConfig, log *logrus.Entry) (*sql.DB, error) {
	source := fmt.Sprintf("host=%s port=%d dbname=%s connect_timeout=%d", config.Host, config.Port, config.DB, config.Timeout)
	if config.User != nil {
		log.Debug("Adding user")
		source += fmt.Sprintf(" user=%s", *config.User)
	}
	log.Infof("Connecting using source: %s", source)

	if config.Pass != nil {
		log.Debug("Adding password")
		source += fmt.Sprintf(" password=%s", *config.Pass)
	}

	return sql.Open("postgres", source)
}

func StartBatcher(db *sql.DB, timeout time.Duration, batchSize int, log *logrus.Entry) (chan<- (*messaging.InboundMetric), chan<- (bool)) {
	batchLock := new(sync.Mutex)
	currentBatch := []*messaging.InboundMetric{}

	incoming := make(chan *messaging.InboundMetric, batchSize)
	shutdown := make(chan bool)
	go func() {
		for {
			select {

			case m := <-incoming:
				batchLock.Lock()
				currentBatch = append(currentBatch, m)
				if len(currentBatch) > batchSize {
					out := currentBatch
					currentBatch = []*messaging.InboundMetric{}
					go sendBatch(db, out, log.WithField("reason", "size"))
				}
				batchLock.Unlock()
			case <-time.After(timeout):
				batchLock.Lock()
				if len(currentBatch) > 0 {
					out := currentBatch
					currentBatch = []*messaging.InboundMetric{}
					go sendBatch(db, out, log.WithField("reason", "timeout"))
				}
				batchLock.Unlock()

			case <-shutdown:
				log.Debug("Got shutdown signal")
				close(shutdown)
				return
			}
		}
		log.Debug("Shutdown batcher")
	}()

	return incoming, shutdown
}

var batchCounter int32

func sendBatch(db *sql.DB, batch []*messaging.InboundMetric, log *logrus.Entry) {
	atomic.AddInt32(&batchCounter, 1)
	start := time.Now()
	log = log.WithField("batch_id", start.UnixNano())
	defer func() {
		inflight := atomic.AddInt32(&batchCounter, -1)
		dur := time.Since(start)
		log.WithFields(logrus.Fields{
			"dur":              dur.Nanoseconds(),
			"inflight_batches": inflight,
		}).Infof("Finished processing batch in %s", dur.String())
	}()

	log.Info("Starting to process batch")
	metricValues := []string{}
	dimValues := []string{}

	for _, m := range batch {
		id := id()
		tail := fmt.Sprintf("('%s', '%s', '%s', %d, '%s', default)", id, m.Name, m.Timestamp.Format(time.RFC822Z), m.Value, m.Type)
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
				default:
					log.Warnf("Unsupported type for dim %s: %v : %v", k, t, v)
					continue
				}
				dimValues = append(dimValues, fmt.Sprintf("('%s', '%s', '%s')", k, asStr, id))
			}
		}
	}

	log.WithFields(logrus.Fields{
		"batch_size":    len(batch),
		"metrics_count": len(metricValues),
		"dims_count":    len(dimValues),
	}).Info("Storing batch")

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

func insert(tx *sql.Tx, root string, entries []string, log *logrus.Entry) error {
	start := time.Now()
	rows := int64(0)
	defer func() {
		dur := time.Since(start)
		log.WithFields(logrus.Fields{
			"affected_rows": rows,
			"dur":           dur.Nanoseconds(),
		}).Debugf("Stored %d rows in %s", rows, dur.String())
	}()

	stmt := root + strings.Join(entries, ",")
	if LogQueries {
		log.WithField("query", true).Info(stmt)
	}
	res, err := tx.Exec(stmt)
	if err != nil {
		log.WithError(err).Warn("Failed to save values in redshift")
		return err
	}

	rows, err = res.RowsAffected()
	if err != nil {
		log.WithError(err).Warn("Failed to get the rows affected")
		return err
	}
	if rows != int64(len(entries)) {
		log.Warnf("Didn't save all the metrics %d vs %d expected", rows, len(entries))
		return errors.New("Incorrect amount saved")
	}

	return nil
}
