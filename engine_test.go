package sqldb_test

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/binwen/sqldb"
	"github.com/binwen/sqldb/config"
)

func TestNewDBEngine(t *testing.T) {
	driver := os.Getenv("Driver")
	masterDns := os.Getenv("Dns")
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
	engine, err := sqldb.OpenDBEngine(
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
				Policy: config.PolicyOptions{
					Mode:   "weightroundrobin",
					Params: sqldb.PolicyParams{Weights: []int{2, 3}},
				},
			},
		},
		showSQL,
	)
	if err != nil {
		t.Error(err)
	}

	if engine.DriverName() != driver {
		t.Errorf("default driver name should `mysql`, got `%v`", engine.DriverName())
	}

	cluster := engine.Use("cluster")
	if _, err := cluster.Table("auth_user").Count(); err != nil {
		t.Error(err)
	}
}

func TestSetPolicy(t *testing.T) {
	driver := os.Getenv("Driver")
	masterDns := os.Getenv("Dns")
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

	engine, err := sqldb.OpenDBEngine(
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
	if err != nil {
		t.Error(err)
	}

	engine.SetPolicy("cluster", sqldb.WeightRoundRobinPolicy(sqldb.PolicyParams{Weights: []int{2, 3}}))
	cluster := engine.Use("cluster")
	if _, err := cluster.Table("auth_user").Count(); err != nil {
		t.Error(err)
	}
}

func TestCustomPolicy(t *testing.T) {
	var fn = func(weights []int) sqldb.PolicyHandler {
		weightsLen := len(weights)
		rands := make([]int, 0, weightsLen)

		for i := 0; i < weightsLen; i++ {
			for n := 0; n < weights[i]; n++ {
				rands = append(rands, i)
			}
		}

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		return func(engine *sqldb.ConnectionEngine) *sqldb.Connection {
			index := rands[r.Intn(len(rands))]
			count := len(engine.Slaves())
			if index >= count {
				index = count - 1
			}

			return engine.Slaves()[index]
		}
	}

	driver := os.Getenv("Driver")
	masterDns := os.Getenv("Dns")
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

	engine, err := sqldb.OpenDBEngine(
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
	if err != nil {
		t.Error(err)
	}

	engine.SetPolicy("cluster", fn([]int{2, 3}))
	cluster := engine.Use("cluster")
	if _, err := cluster.Table("auth_user").Count(); err != nil {
		t.Error(err)
	}
}

func TestRegisterPolicyHandler(t *testing.T) {
	var fn = func(weights []int) sqldb.PolicyHandler {
		weightsLen := len(weights)
		rands := make([]int, 0, weightsLen)

		for i := 0; i < weightsLen; i++ {
			for n := 0; n < weights[i]; n++ {
				rands = append(rands, i)
			}
		}

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		return func(engine *sqldb.ConnectionEngine) *sqldb.Connection {
			index := rands[r.Intn(len(rands))]
			count := len(engine.Slaves())
			if index >= count {
				index = count - 1
			}

			return engine.Slaves()[index]
		}
	}

	sqldb.RegisterPolicyHandler("custom", fn)

	driver := os.Getenv("Driver")
	masterDns := os.Getenv("Dns")
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

	engine, err := sqldb.OpenDBEngine(
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
				Policy: config.PolicyOptions{
					Mode:   "custom",
					Params: []int{2, 3},
				},
			},
		},
		showSQL,
	)
	if err != nil {
		t.Error(err)
	}

	if err != nil {
		t.Error(err)
	}
	cluster := engine.Use("cluster")
	if _, err := cluster.Table("auth_user").Count(); err != nil {
		t.Error(err)
	}
}
