package redshift

import (
	"database/sql"

	"github.com/pkg/errors"
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
)

func CreateTables(db *sql.DB) error {
	if _, err := db.Exec(createMetrics); err != nil {
		return errors.Wrap(err, "creating metrics table")
	}

	if _, err := db.Exec(createDims); err != nil {
		return errors.Wrap(err, "creating dims table")
	}

	return nil
}
