package tests

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/binwen/sqldb"
	_ "github.com/binwen/sqldb/dialects/mysql"
	_ "github.com/binwen/sqldb/dialects/postgres"
	_ "github.com/binwen/sqldb/dialects/sqlite"
)

var (
	DBEngine *sqldb.EngineGroup
	err      error
)

func openDB() (*sqldb.EngineGroup, error) {
	driver := os.Getenv("Driver")
	if driver == "" {
		driver = "sqlite3"
	}
	masterDns := os.Getenv("Dns")
	if masterDns == "" {
		masterDns = "./sqldb.db?cache=shared&mode=rwc"
	}
	showSQL := os.Getenv("ShowSQL") == "true"
	maxConns, _ := strconv.Atoi(os.Getenv("MaxConns"))
	maxIdleConns, _ := strconv.Atoi(os.Getenv("MaxIdleConns"))
	maxLifetime, _ := strconv.Atoi(os.Getenv("MaxLifetime"))

	var slaves []*sqldb.Config
	for _, dns := range strings.Split(os.Getenv("Slaves"), ";") {
		if dns == "" {
			continue
		}
		slaves = append(slaves, &sqldb.Config{
			Driver:       driver,
			DNS:          dns,
			MaxConns:     maxConns,
			MaxIdleConns: maxIdleConns,
			MaxLifetime:  maxLifetime,
		})
	}
	return sqldb.OpenDBEngine(
		sqldb.DBConfig{
			"default": &sqldb.Config{
				Driver:       driver,
				DNS:          masterDns,
				MaxConns:     maxConns,
				MaxIdleConns: maxIdleConns,
				MaxLifetime:  maxLifetime,
			},
			"cluster": &sqldb.ClusterConfig{
				Driver: driver,
				Master: &sqldb.Config{
					DNS:          masterDns,
					MaxConns:     maxConns,
					MaxIdleConns: maxIdleConns,
					MaxLifetime:  maxLifetime,
				},
				Slaves: slaves,
			},
		},
		showSQL,
	)
}

func init() {
	if DBEngine, err = openDB(); err != nil {
		panic(fmt.Sprintf("failed to initialize database, got error %v", err))
	}
}
