package sqldb

import (
	"database/sql"
	"errors"
	"reflect"

	"github.com/jmoiron/sqlx"
)

type RawSession struct {
	query string
	vars  []interface{}
	db    *SqlDB
}

func (raw *RawSession) Fetch(dest interface{}) error {
	destRefValue := reflect.ValueOf(dest)
	if IsNil(destRefValue) {
		return errors.New("nil pointer passed to scan destination")
	}

	rows, err := raw.db.Query(raw.query, raw.vars...)
	if err != nil {
		return err
	}
	defer rows.Close()
	destWrapper := DestWrapper{Dest: dest, ReflectValue: reflect.Indirect(destRefValue)}

	return ScanAll(rows, destWrapper)
}

func (raw *RawSession) Exec() (result sql.Result, err error) {
	return raw.db.Exec(raw.query, raw.vars...)
}

func (raw *RawSession) Query() (rows *sqlx.Rows, err error) {
	return raw.db.Query(raw.query, raw.vars...)
}

func (raw *RawSession) QueryRow() (row *sqlx.Row) {
	return raw.db.QueryRow(raw.query, raw.vars...)
}

func (raw *RawSession) Master() *RawSession {
	raw.db.isMaster = true
	return raw
}
