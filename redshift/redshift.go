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
)

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
		}).Debugf("Stored %d rows in %s", rows, dur.String())
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
