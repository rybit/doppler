package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	_ "github.com/lib/pq" // Postgres driver.
	"github.com/pborman/uuid"
	"github.com/pkg/errors"

	"github.com/rybit/doppler/messaging"
)

type Config struct {
	Host    string  `mapstructure:"host"`
	Port    int     `mapstructure:"port"`
	DB      string  `mapstructure:"db"`
	User    *string `mapstructure:"user"`
	Pass    *string `mapstructure:"pass"`
	Timeout int     `mapstructure:"connect_timeout"`

	MetricsConf *DBIngestConfig `mapstructure:"metrics_conf"`
	LinesConf   *DBIngestConfig `mapstructure:"lines_conf"`
}

type DBIngestConfig struct {
	messaging.IngestConfig `mapstructure:",squash"`
	LogQueries             bool `mapstructure:"log_queries"`
}

func id() string {
	return uuid.NewRandom().String()
}

func ConnectToRedshift(host string, port int, db string, user, pass *string, timeout int) (*sql.DB, error) {
	source := fmt.Sprintf("host=%s port=%d dbname=%s connect_timeout=%d", host, port, db, timeout)
	if user != nil {
		source += fmt.Sprintf(" user=%s", *user)
	}

	if pass != nil {
		source += fmt.Sprintf(" password=%s", *pass)
	}

	return sql.Open("postgres", source)
}

func insert(tx *sql.Tx, root string, entries []string, verbose bool, log *logrus.Entry) error {
	if len(entries) == 0 {
		log.Debug("Skipping insert b/c it is an empty set")
		return nil
	}

	start := time.Now()
	rows := int64(0)
	defer func() {
		dur := time.Since(start)
		log.WithFields(logrus.Fields{
			"affected_rows": rows,
			"dur":           dur.Nanoseconds(),
		}).Infof("Stored %d rows in %s", rows, dur.String())
	}()

	stmt := root + strings.Join(entries, ",")
	if verbose {
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
