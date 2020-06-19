package tests

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"sqldb"
	"sqldb/config"
	_ "sqldb/dialects/mysql"
	_ "sqldb/dialects/postgres"
	_ "sqldb/dialects/sqlite"
)

var (
	DBEngine *sqldb.EngineGroup
	err      error
)

func openDB() (*sqldb.EngineGroup, error) {
	driver := os.Getenv("Driver")
	if driver == "" {
		driver = "mysql"
		//driver = "sqlite3"
		//driver = "postgres"
	}
	masterDns := os.Getenv("Dns")
	if masterDns == "" {
		masterDns = "root:@/sqldb?charset=utf8&parseTime=True"
		//masterDns = "postgres://postgres:xtx123@10.0.3.3:5432/sqldb?sslmode=disable"
		//masterDns = "host=10.0.3.3 port=5432 user=postgres dbname=sqldb sslmode=disable password=xtx123"
		//masterDns = "./sqldb.db?cache=shared&mode=rwc"
	}
	showSQL := true // os.Getenv("ShowSQL") == "true"
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
