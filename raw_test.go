package sqldb_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/binwen/sqldb"
	"github.com/binwen/sqldb/tests"
)

func TestFetchScanMap(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6)
		var m = map[string]interface{}{}
		if err := tests.DBEngine.Raw("select * from auth_user").Fetch(m); err != nil {
			t.Error(err)
		} else {
			var userId int64
			if id, ok := m["id"].(string); ok {
				userId, _ = strconv.ParseInt(id, 0, 64)
			} else if id, ok := m["id"].(int64); ok {
				userId = id
			}

			if userId != 1 {
				t.Errorf("This user id value should be `1`, got %v", userId)
			}
		}

		var m1 = map[string]interface{}{}
		if err := tests.DBEngine.Raw("select * from auth_user where id = ?", 2).Fetch(&m1); err != nil {
			t.Error(err)
		} else {
			userId, _ := m1["id"].(int64)
			if userId != 2 {
				t.Errorf("This user id value should be `2`, got %v", userId)
			}
		}

		var m2 = map[string]interface{}{}
		if err := tests.DBEngine.Raw("select id from auth_user where id in (?)", []int{1, 2, 3}).Fetch(&m2); err != nil {
			t.Error(err)
		} else {
			userId, _ := m2["id"].(int64)
			if userId != 1 {
				t.Errorf("This user id value should be `1`, got %v", userId)
			}
		}

		var noFound = map[string]interface{}{}
		if err := tests.DBEngine.Raw("select * from auth_user where id = ?", -1).Fetch(&noFound); err == nil {
			t.Errorf("map dest raw fetch query should be `sqldb.ErrRecordNotFound` error")
		} else {
			if err != sqldb.ErrRecordNotFound {
				t.Errorf("map dest raw fetch query error should be `sqldb.ErrRecordNotFound`, got `%v`", err)
			}
		}

		var nilMap map[string]interface{}
		if err := tests.DBEngine.Raw("select * from auth_user").Fetch(nilMap); err == nil {
			t.Errorf("nil map dest raw fetch query should be error")
		}

		var nilMapPtr map[string]interface{}
		if err := tests.DBEngine.Raw("select * from auth_user").Fetch(&nilMapPtr); err == nil {
			t.Errorf("nil map ptr dest raw fetch query should be error")
		}
	})
}

func TestFetchScanMapSlice(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6)
		var mps1 []map[string]interface{}
		if err := tests.DBEngine.Raw("select id, username from auth_user limit 2").Fetch(&mps1); err != nil {
			t.Error(err)
		} else {
			if len(mps1) != 2 {
				t.Errorf("len should 2, got %v", len(mps1))
			}
		}

		var mps3 []*map[string]interface{}
		if err := tests.DBEngine.Raw("select id, username from auth_user limit 2").Fetch(&mps3); err != nil {
			t.Error(err)
		} else {
			if len(mps3) != 2 {
				t.Errorf("len should 2, got %v", len(mps3))
			}
			var userId int64
			if id, ok := (*mps3[0])["id"].(string); ok {
				userId, _ = strconv.ParseInt(id, 0, 64)
			} else if id, ok := (*mps3[0])["id"].(int64); ok {
				userId = id
			}

			if userId != 1 {
				t.Errorf("This user id value should be `1`, got %v", userId)
			}
		}

		var mps4 *[]*map[string]interface{}
		if err := tests.DBEngine.Raw("select id, username from auth_user limit 2").Fetch(&mps4); err != nil {
			t.Error(err)
		} else if mps4 != nil {
			t.Errorf("should be nil, got %v", mps4)
		}

		var mps2 []map[string]interface{}
		if err := tests.DBEngine.Raw("select id, username from auth_user limit 2").Fetch(mps2); err == nil {
			t.Errorf("map slice dest raw fetch query should error")
		}
	})
}

func TestFetchScanStruct(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6)
		var u1 tests.AuthUser
		if err := tests.DBEngine.Raw("select id, username from auth_user where id=?", 1).Fetch(&u1); err != nil {
			t.Error(err)
		} else {
			if u1.Id != 1 {
				t.Errorf("This user id should be `1`, got %v", u1.Id)
			}
		}

		var u2 = tests.AuthUser{}
		if err := tests.DBEngine.Raw("select id, username from auth_user where id=?", 3).Fetch(&u2); err != nil {
			t.Error(err)
		} else {
			if u2.Id != 3 {
				t.Errorf("This user id should be `3`, got %v", u2.Id)
			}
		}

		var u5 = &tests.AuthUser{}
		if err := tests.DBEngine.Raw("select id, username from auth_user where id=?", 6).Fetch(&u5); err != nil {
			t.Error(err)
		} else {
			if u5.Id != 6 {
				t.Errorf("This user id should be `6`, got %v", u5.Id)
			}
		}

		var u6 = &tests.AuthUser{}
		if err := tests.DBEngine.Raw("select id, username from auth_user where id=?", 6).Fetch(u6); err != nil {
			t.Error(err)
		} else {
			if u6.Id != 6 {
				t.Errorf("This user id should be `6`, got %v", u6.Id)
			}
		}

		var u3 tests.AuthUser
		if err := tests.DBEngine.Raw("select id, username from auth_user where id=?", 4).Fetch(u3); err == nil {
			t.Error("should be err, must pass a pointer, not a value, to StructScan destination")
		}

		var u4 = tests.AuthUser{}
		if err := tests.DBEngine.Raw("select id, username from auth_user where id=?", 3).Fetch(u4); err == nil {
			t.Error("should be err, must pass a pointer, not a value, to StructScan destination")
		}
	})
}

func TestFetchScanStructSlice(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6)

		var users []tests.AuthUser
		if err := tests.DBEngine.Raw("select * from auth_user limit 2").Fetch(&users); err != nil {
			t.Error(err)
		} else {
			if users[0].Id != 1 {
				t.Errorf("first id should be `1`, got %v", users[0].Id)
			}
		}

		var users1 []*tests.AuthUser
		if err := tests.DBEngine.Raw("select id, username from auth_user limit 2").Fetch(&users1); err != nil {
			t.Error(err)
		} else {
			if users1[0].Id != 1 {
				t.Errorf("first id should be `1`, got %v", users1[0].Id)
			}
		}

		var users2 []tests.AuthUser
		if err := tests.DBEngine.Raw("select * from auth_user where id in (1,2)").Fetch(users2); err == nil {
			t.Error("should be error, nil pointer passed to scan destination")
		}

		var users3 []*tests.AuthUser
		if err := tests.DBEngine.Raw("select id, username from auth_user limit 2").Fetch(users3); err == nil {
			t.Error("should be error, nil pointer passed to scan destination")
		}
	})
}

func TestFetchScanOther(t *testing.T) {
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

		var names []string
		if err := tests.DBEngine.Raw("select username from auth_user limit 5").Fetch(&names); err != nil {
			t.Error(err)
		} else {
			if len(names) != 5 {
				t.Errorf("len should be `5`, got %v", len(names))
			}

			if names[0] != "user1" {
				t.Errorf("This first username should be `user1`, got %v", names[0])
			}
		}

		var uIds []int64
		if err := tests.DBEngine.Raw("select id from auth_user limit 5").Fetch(&uIds); err != nil {
			t.Error(err)
		} else {
			if len(uIds) != 5 {
				t.Errorf("len should be `5`, got %v", len(uIds))
			}

			if uIds[0] != 1 {
				t.Errorf("first id should be `1`, got %v", uIds[0])
			}
		}
	})
}

func TestLastInsertIdWithRaw(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		if tests.DBEngine.DriverName() != "postgres" {
			if result, err := tests.DBEngine.Raw(
				"insert into auth_user(is_superuser,username,age, date_joined,last_login) values(?,?,?,?,?)",
				1, "user_exec", 18, time.Now(), time.Now(),
			).Exec(); err != nil {
				t.Error(err)
			} else {
				if lastInsertId, err := result.LastInsertId(); err != nil {
					t.Error(err)
				} else if lastInsertId != 1 {
					t.Error("raw exec error")
				}
			}
		} else {
			if row := tests.DBEngine.Raw(
				"insert into auth_user(is_superuser,username,age, date_joined,last_login) values(?,?,?,?,?) RETURNING id",
				1, "user_exec", 18, time.Now(), time.Now(),
			).QueryRow(); row.Err() != nil {
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

func TestExecWithRaw(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		if tests.DBEngine.DriverName() != "postgres" {
			if result, err := tests.DBEngine.Raw(
				"insert into auth_user(is_superuser,username,age, date_joined,last_login) values(?,?,?,?,?)",
				1, "user_exec", 18, time.Now(), time.Now(),
			).Exec(); err != nil {
				t.Error(err)
			} else {
				if lastInsertId, err := result.LastInsertId(); err != nil {
					t.Error(err)
				} else if lastInsertId != 1 {
					t.Error("raw exec error")
				}
			}
		}
		tests.InsertAuthUserWithId(2)
		if result, err := tests.DBEngine.Raw(
			"update auth_user set age = age + 1 where id = ?",
			2,
		).Exec(); err != nil {
			t.Error(err)
		} else {
			if affected, err := result.RowsAffected(); err != nil {
				t.Error(err)
			} else if affected == 0 {
				t.Error("update user error")
			}
		}
	})
}

func TestQueryWithRaw(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		rows, err := tests.DBEngine.Raw("select * from auth_user").Query()
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

		rows, err = tests.DBEngine.Raw("select username from auth_user").Query()
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

func TestQueryRowWithRaw(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		row := tests.DBEngine.Raw("select * from auth_user where id = ?", 1).QueryRow()
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
