package sqldb_test

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/binwen/sqldb"
	"github.com/binwen/sqldb/tests"
)

func TestOpenDBEngine(t *testing.T) {
	driver := os.Getenv("Driver")
	masterDns := os.Getenv("Dns")
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

	engine, err := sqldb.OpenDBEngine(
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

func TestTable(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		var simpleUser tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select().First(&simpleUser); err != nil {
			t.Error(err)
		} else if simpleUser.Id != 1 {
			t.Errorf("id value should `1`, got `%v`", simpleUser.Id)
		}
	})
}

func TestRaw(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6)
		var count int64
		if err := tests.DBEngine.Raw("select count(*) from auth_user").Fetch(&count); err != nil {
			t.Error(err)
		} else {
			if count != 6 {
				t.Errorf("This count value should be `6`, got %v", count)
			}
		}
	})
}

func TestRebind(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		var result string
		if tests.DBEngine.DriverName() == "postgres" {
			result = "select * from auth_user where user_id = $1"
		} else {
			result = "select * from auth_user where user_id = ?"
		}

		query := tests.DBEngine.Rebind("select * from auth_user where user_id = ?")
		if query != result {
			t.Errorf("sql rebind expects %v got %v", result, query)
		}
	})
}

func TestLastInsertId(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		if tests.DBEngine.DriverName() != "postgres" {
			if result, err := tests.DBEngine.Exec(
				"insert into auth_user(is_superuser,username,age, date_joined,last_login) values(?,?,?,?,?)",
				1, "user_exec", 18, time.Now(), time.Now(),
			); err != nil {
				t.Error(err)
			} else {
				if lastInsertId, err := result.LastInsertId(); err != nil {
					t.Error(err)
				} else if lastInsertId != 1 {
					t.Error("raw exec error")
				}
			}
		} else {
			if row := tests.DBEngine.QueryRow(
				"insert into auth_user(is_superuser,username,age, date_joined,last_login) values(?,?,?,?,?) RETURNING id",
				1, "user_exec", 18, time.Now(), time.Now(),
			); row.Err() != nil {
				t.Error(row.Err())
			} else {
				var lastInsertId int64
				err := row.Scan(&lastInsertId)
				if err != nil {
					t.Error(err)
				}
				if lastInsertId != 1 {
					t.Error("raw exec error")
				}
			}
		}
	})
}

func TestExec(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		if tests.DBEngine.DriverName() != "postgres" {
			if result, err := tests.DBEngine.Exec(
				"insert into auth_user(is_superuser,username, age, date_joined,last_login) values(?,?,?,?,?)",
				1, "user1", 18, time.Now(), time.Now(),
			); err != nil {
				t.Error(err)
			} else {
				if id, err := result.LastInsertId(); err != nil {
					t.Error(err)
				} else if id != 1 {
					t.Error("lastInsertId error")
				}
			}
		}
		tests.InsertAuthUserWithId(2, 3, 4)
		if result, err := tests.DBEngine.Exec(
			"update auth_user set age = age + 1 where id in (?)",
			[]int64{2, 4},
		); err != nil {
			t.Error(err)
		} else {
			if affected, err := result.RowsAffected(); err != nil {
				t.Error(err)
			} else if affected != 2 {
				t.Error("update user error")
			}
		}
	})
}

func TestQuery(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		rows, err := tests.DBEngine.Query("select * from auth_user")
		if err != nil {
			t.Error(err)
		}
		defer rows.Close()
		for rows.Next() {
			user := &tests.AuthUser{}
			if err := rows.StructScan(user); err != nil {
				t.Error(err)
			}
		}

		rows, err = tests.DBEngine.Query("select username from auth_user")
		if err != nil {
			t.Error(err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
		}
	})
}

func TestQueryRow(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		row := tests.DBEngine.QueryRow("select * from auth_user where id = ?", 1)
		var user = tests.AuthUser{}
		err := row.StructScan(&user)
		if err != nil {
			t.Error(err)
		}
		if user.Id != 1 {
			t.Error("wrapper QueryRowx error")
		}
	})
}

func TestQueryIn(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3)
		rows, err := tests.DBEngine.Query("select * from auth_user where age=? and id in (?)", 18, []int{2, 3})
		if err != nil {
			t.Error(err)
		}
		defer rows.Close()
		for rows.Next() {
			user := &tests.AuthUser{}
			if err := rows.StructScan(user); err != nil {
				t.Error(err)
			}
		}
	})
}

func TestTx(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		err := tests.DBEngine.Tx(func(tx *sqldb.SqlDB) error {
			for id := 1; id < 10; id++ {
				user := &tests.AuthUser{
					UserName:    "user4",
					IsSuperuser: true,
					Age:         18,
					ModelTime: tests.ModelTime{
						DateJoined: time.Now(),
					},
				}

				_, _ = tx.Table("auth_user").Create(user)

				if id == 8 {
					return errors.New("simulation terminated")
				}
			}
			return nil
		})
		if err == nil {
			t.Error("transaction abort failed")
		}
		num, err := tests.DBEngine.Table("auth_user").Count()

		if err != nil {
			t.Error(err)
		}

		if num != 0 {
			t.Error("transaction abort failed")
		}

		err1 := tests.DBEngine.Tx(func(tx *sqldb.SqlDB) error {
			for id := 1; id < 10; id++ {
				user := &tests.AuthUser{
					UserName:    "user4",
					IsSuperuser: true,
					Age:         18,
					ModelTime: tests.ModelTime{
						DateJoined: time.Now(),
					},
				}

				_, _ = tx.Table("auth_user").Create(user)
			}
			return nil
		})

		num1, err1 := tests.DBEngine.Table("auth_user").Count()

		if err1 != nil {
			t.Error(err)
		}

		if num1 != 9 {
			t.Error("transaction abort failed")
		}

	})
}

func TestWithTx(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		err := tests.DBEngine.Tx(func(tx *sqldb.SqlDB) error {
			for id := 1; id < 10; id++ {
				_, err := tx.Exec("insert into auth_user(is_superuser,username,age,date_joined) values(?,?,?,?)", 1, "user", 18, time.Now())
				if err != nil {
					return err
				}
			}

			var num int
			err := tx.QueryRow("select count(*) from auth_user").Scan(&num)

			if err != nil {
				return err
			}

			if num != 9 {
				t.Error("with transaction create failed")
			}

			return nil
		})

		if err != nil {
			t.Fatalf("with transaction failed %s", err)
		}
	})
}

func TestBegin(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tx, err := tests.DBEngine.Begin()
		if err != nil {
			t.Fatalf("with transaction begin error %s", err)
		}

		var fn = func() error {
			for id := 1; id < 10; id++ {
				_, err := tx.Exec("insert into auth_user(is_superuser,username,age,date_joined) values(?,?,?,?)", 1, "user", 18, time.Now())
				if err != nil {
					return err
				}
			}

			var num int
			err := tx.QueryRow("select count(*) from auth_user").Scan(&num)

			if err != nil {
				return err
			}

			if num != 9 {
				t.Error("with transaction create failed")
			}

			return nil
		}

		err = fn()
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				t.Fatalf("with transaction rollback error %s", err)
			}
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("with transaction commit error %s", err)
		}
	})
}
