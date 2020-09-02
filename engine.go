package sqldb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/binwen/sqldb/dialects"
	"github.com/binwen/sqldb/logger"
)

const DefaultDBAlias = "default"

type Connection struct {
	*sqlx.DB
}

type ConnectionEngine struct {
	master    *Connection
	slaves    []*Connection
	policy    IPolicy
	Dialector dialects.Dialector
}

func (engine *ConnectionEngine) Slave() *Connection {
	switch len(engine.slaves) {
	case 0:
		return engine.master
	case 1:
		return engine.slaves[0]
	}

	return engine.policy.Slave(engine)
}

func (engine *ConnectionEngine) Master() *Connection {
	return engine.master
}

func (engine *ConnectionEngine) Slaves() []*Connection {
	return engine.slaves
}

func openConnector(dbConf *Config) (*Connection, error) {
	db, err := sqlx.Connect(dbConf.Driver, dbConf.DNS)
	if err != nil {
		return nil, err
	}

	// 连接池设置
	if dbConf.MaxConns > 0 {
		db.SetMaxOpenConns(dbConf.MaxConns)
	}

	if dbConf.MaxIdleConns > 0 {
		db.SetMaxIdleConns(dbConf.MaxIdleConns)
	}

	if dbConf.MaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(dbConf.MaxLifetime) * time.Second)
	}

	return &Connection{db}, nil
}

type EngineGroup struct {
	engineGroup  map[string]*ConnectionEngine
	defaultSqlDB *SqlDB
	showSQL      bool
}

func (eg *EngineGroup) Use(dbAlias ...string) *SqlDB {
	dbName := DefaultDBAlias
	if dbAlias != nil {
		dbName = dbAlias[0]
	}
	engine, ok := eg.engineGroup[dbName]
	if !ok {
		panic(fmt.Sprintf("the database alias `%s` is not configured", dbName))
	}
	return NewSqlDB(engine, eg.showSQL)
}

func (eg *EngineGroup) resolveSingle(dbAlias string, conf *Config) error {
	return eg.resolveCluster(dbAlias, &ClusterConfig{Driver: conf.Driver, Master: conf})
}

func (eg *EngineGroup) resolveCluster(dbAlias string, conf *ClusterConfig) error {
	if conf.Driver == "" {
		return errors.New(fmt.Sprintf("This alias database `%s` conf driver is null", dbAlias))
	}

	dial, ok := dialects.GetDialector(conf.Driver)
	if !ok {
		return errors.New(fmt.Sprintf("dialect %s Not Found", conf.Driver))
	}
	conf.Master.Driver = conf.Driver
	db, err := openConnector(conf.Master)
	if err != nil {
		return err
	}

	engine, ok := eg.engineGroup[dbAlias]
	if !ok {
		engine = new(ConnectionEngine)
	}

	engine.master = db
	engine.Dialector = dial
	eg.engineGroup[dbAlias] = engine

	if len(conf.Slaves) == 0 {
		return nil
	}

	for _, item := range conf.Slaves {
		item.Driver = conf.Driver
		db, err := openConnector(item)
		if err != nil {
			return err
		}

		engine, _ := eg.engineGroup[dbAlias]
		engine.slaves = append(engine.slaves, db)
		eg.engineGroup[dbAlias] = engine
	}
	if conf.Policy == nil || conf.Policy.Mode == "" {
		eg.engineGroup[dbAlias].policy = RandomPolicy()
	} else {
		handlerFunc, ok := GetPolicyHandler(conf.Policy.Mode)
		if !ok {
			return errors.New(fmt.Sprintf("The policy %s doesn't exist", conf.Policy.Mode))
		}

		fv := reflect.ValueOf(handlerFunc)
		if fv.Kind() != reflect.Func {
			return errors.New(fmt.Sprintf("The policy `%s` not is function", reflect.TypeOf(handlerFunc)))
		}

		if conf.Policy.Params != nil {
			paramList := []reflect.Value{
				reflect.ValueOf(conf.Policy.Params),
			}
			eg.engineGroup[dbAlias].policy = fv.Call(paramList)[0].Interface().(PolicyHandler)
		} else {
			eg.engineGroup[dbAlias].policy = fv.Call(nil)[0].Interface().(PolicyHandler)
		}
	}

	return nil
}

func (eg *EngineGroup) SetPolicy(dbAlias string, policy IPolicy) *EngineGroup {
	engine, ok := eg.engineGroup[dbAlias]
	if !ok {
		panic(fmt.Sprintf("the database alias `%s` is not configured", dbAlias))
	}
	engine.policy = policy

	return eg
}

func (eg *EngineGroup) Close() {
	for _, engine := range eg.engineGroup {
		if err := engine.master.Close(); err != nil {
			logger.Error("Failed to close database")
		}

		for i := 0; i < len(engine.slaves); i++ {
			if err := engine.slaves[i].Close(); err != nil {
				logger.Error("Failed to close slave database")
			}
		}
	}
}

func (eg *EngineGroup) Table(table string) *Session {
	return eg.defaultSqlDB.Table(table)
}

func (eg *EngineGroup) TableContext(ctx context.Context, table string) *Session {
	return eg.defaultSqlDB.TableContext(ctx, table)
}

func (eg *EngineGroup) Raw(query string, args ...interface{}) *RawSession {
	return eg.defaultSqlDB.Raw(query, args...)
}

func (eg *EngineGroup) RawContext(ctx context.Context, query string, args ...interface{}) *RawSession {
	return eg.defaultSqlDB.RawContext(ctx, query, args...)
}

func (eg *EngineGroup) Rebind(query string) string {
	return eg.defaultSqlDB.Rebind(query)
}

func (eg *EngineGroup) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	return eg.defaultSqlDB.Exec(query, args...)
}

func (eg *EngineGroup) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	return eg.defaultSqlDB.ExecContext(ctx, query, args...)
}

func (eg *EngineGroup) Query(query string, args ...interface{}) (rows *sqlx.Rows, err error) {
	return eg.defaultSqlDB.Query(query, args...)
}

func (eg *EngineGroup) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sqlx.Rows, err error) {
	return eg.defaultSqlDB.QueryContext(ctx, query, args...)
}

func (eg *EngineGroup) QueryRow(query string, args ...interface{}) (row *sqlx.Row) {
	return eg.defaultSqlDB.QueryRow(query, args...)
}

func (eg *EngineGroup) QueryRowContext(ctx context.Context, query string, args ...interface{}) (row *sqlx.Row) {
	return eg.defaultSqlDB.QueryRowContext(ctx, query, args...)
}

func (eg *EngineGroup) Tx(fn func(db *SqlDB) error) (err error) {
	return eg.defaultSqlDB.Tx(fn)
}

func (eg *EngineGroup) TxContext(ctx context.Context, fn func(db *SqlDB) error) (err error) {
	return eg.defaultSqlDB.TxContext(ctx, fn)
}

func (eg *EngineGroup) Begin() (*SqlDB, error) {
	return eg.defaultSqlDB.Begin()
}

func (eg *EngineGroup) BeginContext(ctx context.Context) (*SqlDB, error) {
	return eg.defaultSqlDB.BeginContext(ctx)
}

func (eg *EngineGroup) Commit() error {
	return eg.defaultSqlDB.Commit()
}

func (eg *EngineGroup) Rollback() error {
	return eg.defaultSqlDB.Rollback()
}

func (eg *EngineGroup) DriverName() string {
	return eg.defaultSqlDB.DriverName()
}

func NewDBEngineGroup(conf DBConfig, showSQL bool) (engineGroup *EngineGroup, err error) {
	if len(conf) == 0 {
		return nil, errors.New("database connection configuration cannot be empty")
	}
	if _, ok := conf[DefaultDBAlias]; !ok {
		return nil, errors.New(fmt.Sprintf("you must define a '%s' database", DefaultDBAlias))
	}

	engineGroup = &EngineGroup{
		engineGroup: map[string]*ConnectionEngine{},
		showSQL:     showSQL,
	}
	for dbAlias, dbConfig := range conf {
		switch dbConfig.(type) {
		case *Config:
			err = engineGroup.resolveSingle(dbAlias, dbConfig.(*Config))
		case Config:
			conf := dbConfig.(Config)
			err = engineGroup.resolveSingle(dbAlias, &conf)
		case *ClusterConfig:
			err = engineGroup.resolveCluster(dbAlias, dbConfig.(*ClusterConfig))
		case ClusterConfig:
			conf := dbConfig.(ClusterConfig)
			err = engineGroup.resolveCluster(dbAlias, &conf)
		default:
			panic(fmt.Sprintf("OpenDBEngine() need Config or ClusterConfig type param, but gov %T",
				dbConfig))
		}
		if err != nil {
			return
		}
	}

	engineGroup.defaultSqlDB = engineGroup.Use()

	runtime.SetFinalizer(engineGroup, func(engineGroup *EngineGroup) {
		engineGroup.Close()
	})
	return
}
