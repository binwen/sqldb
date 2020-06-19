package tests

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/binwen/sqldb"
	"github.com/binwen/sqldb/config"
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

	var slaves []*config.Config
	for _, dns := range strings.Split(os.Getenv("Slaves"), ";") {
		if dns == "" {
			continue
		}
		slaves = append(slaves, &config.Config{
			Driver:       driver,
			DNS:          dns,
			MaxConns:     maxConns,
			MaxIdleConns: maxIdleConns,
			MaxLifetime:  maxLifetime,
		})
	}
	return sqldb.OpenDBEngine(
		config.DBConfig{
			"default": &config.Config{
				Driver:       driver,
				DNS:          masterDns,
				MaxConns:     maxConns,
				MaxIdleConns: maxIdleConns,
				MaxLifetime:  maxLifetime,
			},
			"cluster": &config.ClusterConfig{
				Driver: driver,
				Master: &config.Config{
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
