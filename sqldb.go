package sqldb

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/binwen/sqldb/clause"
	"github.com/binwen/sqldb/config"
	"github.com/binwen/sqldb/logger"
)

var clauseBuilderMapping = make(map[string]clause.ClauseBuilder, 0)

type SqlDB struct {
	engine   *ConnectionEngine
	tx       *sqlx.Tx
	isMaster bool
	logging  bool
}

type ISqlx interface {
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Rebind(query string) string
	DriverName() string
}

func (db *SqlDB) Table(table string) *Session {
	return NewSession(context.Background(), db, table)
}

func (db *SqlDB) TableContext(ctx context.Context, table string) *Session {
	return NewSession(ctx, db, table)
}

func (db *SqlDB) Raw(query string, args ...interface{}) *RawSession {
	return &RawSession{ctx: context.Background(), db: db, query: query, vars: args}
}

func (db *SqlDB) RawContext(ctx context.Context, query string, args ...interface{}) *RawSession {
	return &RawSession{ctx: ctx, db: db, query: query, vars: args}
}

func (db *SqlDB) Rebind(query string) string {
	return db.engine.Slave().Rebind(query)
}

func (db *SqlDB) getDB() ISqlx {
	if db.tx != nil {
		return db.tx.Unsafe()
	}

	if db.isMaster {
		return db.engine.Master().Unsafe()
	}

	return db.engine.Slave().Unsafe()
}

func (db *SqlDB) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	defer func(start time.Time) {
		logger.ExplainSQL(&logger.QueryStatus{
			Query: query,
			Args:  args,
			Err:   err,
			Start: start,
			End:   time.Now(),
		}, db.logging)

	}(time.Now())

	db.isMaster = true

	query, newArgs := db.convert(query, args)
	return db.getDB().Exec(query, newArgs...)
}

func (db *SqlDB) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	defer func(start time.Time) {
		logger.ExplainSQL(&logger.QueryStatus{
			Query: query,
			Args:  args,
			Err:   err,
			Start: start,
			End:   time.Now(),
		}, db.logging)

	}(time.Now())

	db.isMaster = true

	query, newArgs := db.convert(query, args)
	return db.getDB().ExecContext(ctx, query, newArgs...)
}

func (db *SqlDB) Query(query string, args ...interface{}) (rows *sqlx.Rows, err error) {
	defer func(start time.Time) {
		logger.ExplainSQL(&logger.QueryStatus{
			Query: query,
			Args:  args,
			Err:   err,
			Start: start,
			End:   time.Now(),
		}, db.logging)
	}(time.Now())

	query, newArgs := db.convert(query, args)
	return db.getDB().Queryx(query, newArgs...)
}

func (db *SqlDB) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sqlx.Rows, err error) {
	defer func(start time.Time) {
		logger.ExplainSQL(&logger.QueryStatus{
			Query: query,
			Args:  args,
			Err:   err,
			Start: start,
			End:   time.Now(),
		}, db.logging)
	}(time.Now())

	query, newArgs := db.convert(query, args)
	return db.getDB().QueryxContext(ctx, query, newArgs...)
}

func (db *SqlDB) QueryRow(query string, args ...interface{}) (row *sqlx.Row) {
	defer func(start time.Time) {
		logger.ExplainSQL(&logger.QueryStatus{
			Query: query,
			Args:  args,
			Err:   row.Err(),
			Start: start,
			End:   time.Now(),
		}, db.logging)
	}(time.Now())

	query, newArgs := db.convert(query, args)

	return db.getDB().QueryRowx(query, newArgs...)
}

func (db *SqlDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) (row *sqlx.Row) {
	defer func(start time.Time) {
		logger.ExplainSQL(&logger.QueryStatus{
			Query: query,
			Args:  args,
			Err:   row.Err(),
			Start: start,
			End:   time.Now(),
		}, db.logging)
	}(time.Now())

	query, newArgs := db.convert(query, args)

	return db.getDB().QueryRowxContext(ctx, query, newArgs...)
}

func (db *SqlDB) convert(query string, args []interface{}) (string, []interface{}) {
	var (
		newQuery string
		newArgs  []interface{}
		err      error
	)
	if !IsInsertSQL(query) {
		newQuery, newArgs, err = sqlx.In(query, args...)
	} else {
		newQuery, newArgs = query, args
	}

	if err != nil {
		return query, args
	}

	if i := strings.Index(newQuery, "?"); i != -1 {
		newQuery = db.Rebind(newQuery)
	}

	return newQuery, newArgs
}

func (db *SqlDB) Tx(fn func(db *SqlDB) error) (err error) {
	tx, err := db.engine.Master().Beginx()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				logger.Errorf("sqldb rollback error:%s", err)
			}
		}
	}()

	err = fn(&SqlDB{engine: db.engine, tx: tx, logging: db.logging})
	if err == nil {
		err = tx.Commit()
	}

	return
}

func (db *SqlDB) TxContext(ctx context.Context, fn func(db *SqlDB) error) (err error) {
	tx, err := db.engine.Master().BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				logger.Errorf("sqldb rollback error:%s", err)
			}
		}
	}()

	err = fn(&SqlDB{engine: db.engine, tx: tx, logging: db.logging})
	if err == nil {
		err = tx.Commit()
	}

	return
}

func (db *SqlDB) Begin() (*SqlDB, error) {
	tx, err := db.engine.Master().Beginx()
	if err != nil {
		return nil, err
	}
	return &SqlDB{engine: db.engine, tx: tx, logging: db.logging}, nil
}

func (db *SqlDB) BeginContext(ctx context.Context) (*SqlDB, error) {
	tx, err := db.engine.Master().BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &SqlDB{engine: db.engine, tx: tx, logging: db.logging}, nil
}

func (db *SqlDB) Commit() error {
	return db.tx.Commit()
}

func (db *SqlDB) Rollback() error {
	return db.tx.Rollback()
}

func (db *SqlDB) DriverName() string {
	if db.tx != nil {
		return db.tx.DriverName()
	}
	return db.engine.Master().DriverName()
}

func NewSqlDB(engine *ConnectionEngine, logging bool) *SqlDB {
	return &SqlDB{engine: engine, logging: logging}
}

func OpenDBEngine(conf config.DBConfig, showSQL bool) (*EngineGroup, error) {
	return NewDBEngineGroup(conf, showSQL)
}

func OpenSingleDBEngine(conf *config.Config, showSQL bool) (*EngineGroup, error) {
	return NewDBEngineGroup(config.DBConfig{DefaultDBAlias: conf}, showSQL)
}

func SetClauseBuilder(name string, builder clause.ClauseBuilder) {
	clauseBuilderMapping[name] = builder
}
