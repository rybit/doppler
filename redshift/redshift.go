package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	_ "github.com/lib/pq" // Postgres driver.
	"github.com/pkg/errors"

	"github.com/pborman/uuid"
	"github.com/rybit/doppler/messaging"
)

const (
	metricInsert = `
	insert into metrics
		(id, name, timestamp, value, type)
	values ('%s', '%s', '%s', %d, '%s')
	`

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
}

func id() string {
	return uuid.NewRandom().String()
}

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

func Insert(db *sql.DB, metric *messaging.InboundMetric, log *logrus.Entry) error {
	var err error

	// we have to generate our own IDs b/c redshift doesn't support
	// a 'returning' clause.
	id := id()

	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	stmt := fmt.Sprintf(metricInsert, id, metric.Name, metric.Timestamp.Format(time.RFC822Z), metric.Value, metric.Type)
	log.Debugf("inserting metric: %s", stmt)
	if _, err = tx.Exec(stmt); err != nil {
		tx.Rollback()
		return errors.Wrap(err, "Inserting metric")
	}

	if metric.Dims != nil {
		args := []string{}
		for k, v := range *metric.Dims {
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
			args = append(args, fmt.Sprintf("('%s', '%s', '%s')", k, asStr, id))
		}
		stmt = dimInsert + strings.Join(args, ",")
		log.Debugf("inserting dims: %s", stmt)
		if _, err := tx.Exec(stmt); err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "saving dims")
		}
	}

	return tx.Commit()
}
