package sqldb_test

import (
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/binwen/sqldb"
	"github.com/binwen/sqldb/tests"
)

func TestSelect(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		var SimpleAuthUser tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select().Hint("/*+TDDL:slave()*/").First(&SimpleAuthUser); err != nil {
			t.Error(err)
		} else if SimpleAuthUser.Id != 1 {
			t.Errorf("id value should `1`, got `%v`", SimpleAuthUser.Id)
		}

		var SimpleAuthUser1 tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id", "username").First(&SimpleAuthUser1); err != nil {
			t.Error(err)
		} else if SimpleAuthUser.Id != 1 {
			t.Errorf("id value should `1`, got `%v`", SimpleAuthUser.Id)
		}

		var users []tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Find(&users); err != nil {
			t.Error(err)
		} else if users[0].UserName != "user1" {
			t.Errorf("user name value should `test1`, got `%v`", users[0].UserName)
		}
	})
}

func TestSelectExpr(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		var age int
		if err := tests.DBEngine.Table("auth_user").SelectExpr("COALESCE(age,?)", 12).First(&age); err != nil {
			t.Error(err)
		} else if age != 18 {
			t.Errorf("user age value should `18`, got `%v`", age)
		}
		var users []map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").Select("username").SelectExpr("COALESCE(age,?) as age", 12).Find(&users); err != nil {
			t.Error(err)
		} else if users[0]["age"].(int64) != 18 {
			t.Errorf("user age value should `18`, got `%v`", users[0]["age"].(int64))
		}
	})
}

func TestDistinct(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithName("user2", "user1", "user1", "user1", "user2", "user3")
		var users []map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").Distinct().Select("username").Find(&users); err != nil {
			t.Error(err)
		} else if len(users) != 3 {
			t.Errorf("user find count should `3`, got `%v`", len(users))
		}

		var users1 []map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").Distinct("username", "age").Select("is_superuser").Find(&users1); err != nil {
			t.Error(err)
		} else if len(users1) != 3 {
			t.Errorf("user find count should `3`, got `%v`", len(users1))
		}
	})
}

func TestSelectAndDistinctAndSelectExpr(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithName("user2", "user1", "user1", "user1", "user2", "user3")
		var users []map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").Distinct("username", "age").SelectExpr("COALESCE(last_login,?) as last_login", "2020-06-01 01:00:00").Select("is_superuser").Find(&users); err != nil {
			t.Error(err)
		} else if len(users) != 3 {
			t.Errorf("user find count should `3`, got `%v`", len(users))
		}
	})
}

func TestLimit(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3)

		var SimpleAuthUser []tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select().Limit(2).Find(&SimpleAuthUser); err != nil {
			t.Error(err)
		} else {
			if len(SimpleAuthUser) != 2 {
				t.Errorf("len value should `2`, got `%v`", len(SimpleAuthUser))
			}
			if SimpleAuthUser[1].Id != 2 {
				t.Errorf("id value should `2`, got `%v`", SimpleAuthUser[1].Id)
			}
		}

		if count, err := tests.DBEngine.Table("auth_user").Limit(-1).Count(); err != nil {
			t.Error(err)
		} else if count != 3 {
			t.Errorf("count should `3`, got `%v`", count)
		}
	})
}

func TestOffset(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 7)
		var SimpleAuthUser []tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select().Limit(2).Offset(5).Find(&SimpleAuthUser); err != nil {
			t.Error(err)
		} else {
			if len(SimpleAuthUser) != 2 {
				t.Errorf("len value should `2`, got `%v`", len(SimpleAuthUser))
			}
			if SimpleAuthUser[0].Id != 6 {
				t.Errorf("id value should `6`, got `%v`", SimpleAuthUser[0].Id)
			}
		}

		if count, err := tests.DBEngine.Table("auth_user").Offset(-1).Count(); err != nil {
			t.Error(err)
		} else if count != 7 {
			t.Errorf("count should `7`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Offset(5).Count(); err != nil {
			t.Error(err)
		} else if count != 7 {
			t.Errorf("count should `7`, got `%v`", count)
		}

	})
}

func TestGroupBy(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithName("user1", "user1", "user1", "user2", "user2", "user3")
		var m = make(map[string]interface{})
		if err := tests.DBEngine.Table("auth_user").Select("username, count(*) as total").Where(
			"username=?", "user1").GroupBy("username").First(&m); err != nil {
			t.Error(err)
		}

		if m["username"] != "user1" || (m["total"].(int64)) != 3 {
			t.Errorf("name should be `test1`, but got %v, total should be `3`, but got %v", m["username"], m["total"])
		}

		if err := tests.DBEngine.Table("auth_user").Select("username, count(*) as total").Where(
			"username like ?", "user%",
		).GroupBy("username").Having("username = ?", "user2").First(&m); err != nil {
			t.Error(err)
		}
		if m["username"] != "user2" || (m["total"].(int64)) != 2 {
			t.Errorf("name should be `test2`, but got `%v`, total should be `2`, but got `%v`", m["username"], m["total"])
		}
	})
}

func TestJoin(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4)
		tests.InsertAuthGroup(1, 3, 4)
		tests.InsertUserGroup(1, 1)
		tests.InsertUserGroup(2, 3)
		tests.InsertUserGroup(4, 4)

		var users1 []tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").Join(
			"join auth_user_groups on auth_user_groups.user_id = auth_user.id",
		).Where("auth_user.username like ?", "user%").Find(&users1); err != nil {
			t.Error(err)
		} else if len(users1) != 3 {
			t.Errorf("should find users count be 3 using jojn, but got %v", len(users1))
		}

		var users2 []map[string]interface{}
		if err := tests.DBEngine.Table("auth_user as u").Join(
			"left join auth_user_groups as ug on ug.user_id = u.id and ug.group_id=?",
			3,
		).Where("u.username like ?", "user%").Find(&users2); err != nil {
			t.Error(err)
		} else {
			if len(users2) != 4 {
				t.Errorf("should find users count be 4 using left jojn, but got %v", len(users2))
			}
			user := users2[1]
			if user["username"] != "user2" {
				t.Errorf("user name should be `user2`, but got %v", user["username"])
			}
			if user["group_id"].(int64) != 3 {
				t.Errorf("user group id should be `3`, but got %v", user["group_id"])
			}
		}

		type UserInfo struct {
			UserId    uint64
			GroupName sql.NullString `db:"group_name"`
			UserName  string
		}

		var userInfo UserInfo
		if err := tests.DBEngine.Table("auth_user as u").Select(
			"u.id as userid, g.name as group_name", "u.username",
		).Join(
			"left join auth_user_groups as ug on ug.user_id = u.id",
		).Join("left join auth_group as g on g.id = ug.group_id and g.name=?", "group2").Where(
			"u.username = ?", "user3",
		).First(&userInfo); err != nil {
			t.Error(err)
		} else {
			if userInfo.UserName != "user3" {
				t.Errorf("user name should be `user3`, but got %v", userInfo.UserName)
			}
			if userInfo.GroupName.String != "" {
				t.Errorf("user group name should be nil, but got %v", userInfo.GroupName.String)
			}
		}
	})
}

func TestWhere(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
		var user tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").Where("username=?", "user1").First(&user); err != nil {
			t.Error(err)
		} else if user.Id != 1 {
			t.Errorf("user id value should `1`, got `%v`", user.Id)
		}

		var users []tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").Where("id <> ?", 1).Find(&users); err != nil {
			t.Error(err)
		} else {
			if len(users) != 11 {
				t.Errorf("users len value should `11`, got `%v`", len(users))
			}

			if users[0].Id != 2 {
				t.Errorf("users first user id value should `2`, got `%v`", users[0].Id)
			}
		}

		if count, err := tests.DBEngine.Table("auth_user").Where("id  in (?)", []int{2, 3}).Count(); err != nil {
			t.Error(err)
		} else if count != 2 {
			t.Errorf("query count value should `2`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where("id  IN ?", []int{2, 3}).Count(); err != nil {
			t.Error(err)
		} else if count != 2 {
			t.Errorf("query count value should `2`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where("username like ?", "user1%").Count(); err != nil {
			t.Error(err)
		} else if count != 4 {
			t.Errorf("fuzzy query count value should `4`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where(map[string]interface{}{"username": "user1", "age": 18}).Count(); err != nil {
			t.Error(err)
		} else if count != 1 {
			t.Errorf("map where query count value should `1`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where(map[interface{}]interface{}{"username": "user1", "age": 18}).Count(); err != nil {
			t.Error(err)
		} else if count != 1 {
			t.Errorf("map where query count value should `1`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where(map[string]string{"username": "user1", "age": "18"}).Count(); err != nil {
			t.Error(err)
		} else if count != 1 {
			t.Errorf("map where query count value should `1`, got `%v`", count)
		}

		if _, err := tests.DBEngine.Table("auth_user").Where(&tests.AuthUser{UserName: "user1"}).Count(); err == nil {
			t.Errorf("struct where query should be error, but got success")
		}

	})
}

func TestOrderBy(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithName(
			"user1", "user2", "user3", "user4", "user5", "user6",
			"user7", "user8", "user9", "user10", "user11", "user12",
			"user1", "user1", "user2", "user2", "user3", "user4",
		)
		var users []tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").OrderBy("username desc, id").Find(&users); err != nil {
			t.Error(err)
		} else {
			if users[0].UserName != "user9" {
				t.Errorf("user name should `user9`, got `%v`", users[0].UserName)
			}
			lastUser := users[len(users)-1]
			if lastUser.UserName != "user1" || lastUser.Id != 14 {
				t.Errorf("last user name should `user1`, but got `%v`; user id should `14`, but got `%v`", lastUser.UserName, lastUser.Id)
			}
		}

		var users1 []tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").OrderBy("username desc").OrderBy("id").Find(&users1); err != nil {
			t.Error(err)
		} else {
			if users[0].UserName != "user9" {
				t.Errorf("user name should `user9`, got `%v`", users[0].UserName)
			}
			lastUser := users[len(users)-1]
			if lastUser.UserName != "user1" || lastUser.Id != 14 {
				t.Errorf("last user name should `user1`, but got `%v`; user id should `14`, but got `%v`", lastUser.UserName, lastUser.Id)
			}
		}

	})
}

func TestAsc(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithName(
			"user1", "user2", "user3", "user4", "user5", "user6",
			"user7", "user8", "user9", "user10", "user11", "user12",
			"user1", "user1", "user2", "user2", "user3", "user4",
		)
		var users []tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").Asc("username", "id").Find(&users); err != nil {
			t.Error(err)
		} else {
			if users[0].UserName != "user1" {
				t.Errorf("user name should `user1`, got `%v`", users[0].UserName)
			}
			lastUser := users[len(users)-1]
			if lastUser.UserName != "user9" || lastUser.Id != 9 {
				t.Errorf("last user name should `user9`, but got `%v`; user id should `9`, but got `%v`", lastUser.UserName, lastUser.Id)
			}
		}

		var users1 []tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").OrderBy("username desc").Asc("id").Find(&users1); err != nil {
			t.Error(err)
		} else {
			if users1[0].UserName != "user9" {
				t.Errorf("user name should `test9`, got `%v`", users1[0].UserName)
			}
			lastUser := users1[len(users)-1]
			if lastUser.UserName != "user1" || lastUser.Id != 14 {
				t.Errorf("last user name should `test1`, but got `%v`; user id should `14`, but got `%v`", lastUser.UserName, lastUser.Id)
			}
		}
	})
}

func TestDesc(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithName(
			"user1", "user2", "user3", "user4", "user5", "user6",
			"user7", "user8", "user9", "user10", "user11", "user12",
			"user1", "user1", "user2", "user2", "user3", "user4",
		)
		var users []tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").Desc("username", "id").Find(&users); err != nil {
			t.Error(err)
		} else {
			if users[0].UserName != "user9" {
				t.Errorf("user name should `user9`, got `%v`", users[0].UserName)
			}
			lastUser := users[len(users)-1]
			if lastUser.UserName != "user1" || lastUser.Id != 1 {
				t.Errorf("last user name should `user1`, but got `%v`; user id should `1`, but got `%v`", lastUser.UserName, lastUser.Id)
			}
		}
	})
}

func TestNot(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithName("user1", "user1", "user2", "user2", "user3", "user4")
		if count, err := tests.DBEngine.Table("auth_user").Not("username", "user1").Count(); err != nil {
			t.Error(err)
		} else if count != 4 {
			t.Errorf("query count value should `4`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Not("username", []string{"user1", "user2"}).Count(); err != nil {
			t.Error(err)
		} else if count != 2 {
			t.Errorf("query count value should `2`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Not("username=?", "user2").Where("username in ?", []string{"user1", "user2", "user3"}).Count(); err != nil {
			t.Error(err)
		} else if count != 3 {
			t.Errorf("query count value should `3`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Not(map[string]interface{}{"username": "user1", "age": 17}).Count(); err != nil {
			t.Error(err)
		} else if count != 4 {
			t.Errorf("map not query count value should `4`, got `%v`", count)
		}

		if _, err := tests.DBEngine.Table("auth_user").Not(&tests.AuthUser{UserName: "user1"}).Count(); err == nil {
			t.Errorf("struct not query should be error, but got success")
		}

	})
}

func TestOr(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithName("user1", "user1", "user2", "user2", "user3", "user4")
		if count, err := tests.DBEngine.Table("auth_user").Where("username=?", "user2").Or("username", "user1").Count(); err != nil {
			t.Error(err)
		} else if count != 4 {
			t.Errorf("query count value should `4`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where("username=?", "user1").Or("username", []string{"user3", "user2"}).Count(); err != nil {
			t.Error(err)
		} else if count != 5 {
			t.Errorf("query count value should `5`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Not("username in ?", []string{"user1", "user2"}).Or("username=?", "user2").Count(); err != nil {
			t.Error(err)
		} else if count != 4 {
			t.Errorf("query count value should `4`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where("username=?", "user1").Or(map[string]interface{}{"username": "user4"}).Count(); err != nil {
			t.Error(err)
		} else if count != 3 {
			t.Errorf("map not query count value should `3`, got `%v`", count)
		}

		if _, err := tests.DBEngine.Table("auth_user").Where("username=?", "user1").Or(&tests.AuthUser{UserName: "user1"}).Count(); err == nil {
			t.Errorf("struct not query should be error, but got success")
		}

	})
}

func TestExist(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		if isHas, err := tests.DBEngine.Table("auth_user").Exist(); err != nil {
			t.Error(err)
		} else if !isHas {
			t.Errorf("Should exist, but got `%v`", isHas)
		}

		if isHas, err := tests.DBEngine.Table("auth_user").Where("id = ?", 1).Exist(); err != nil {
			t.Error(err)
		} else if !isHas {
			t.Errorf("Should exist, but got `%v`", isHas)
		}

		if isHas, err := tests.DBEngine.Table("auth_user").Where("id=?", 3).Exist(); err != nil {
			t.Error(t)
		} else if isHas {
			t.Errorf("Should not exist, but got `%v`", isHas)
		}
	})
}

func TestCount(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 7)
		if count, err := tests.DBEngine.Table("auth_user").Count(); err != nil {
			t.Error(err)
		} else if count != 7 {
			t.Errorf("count value should `7`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where("username = ?", "user1").Or("username=?", "user2").Count(); err != nil {
			t.Error(err)
		} else if count != 2 {
			t.Errorf("count value should `2`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Where("username in ?", []string{"user1", "user2", "user2"}).GroupBy("id").Count(); err != nil {
			t.Error(err)
		} else if count != 1 {
			t.Errorf("count value should `1`, got `%v`", count)
		}

		if count, err := tests.DBEngine.Table("auth_user").Select("count(1)").Where("username in ?", []string{"user1", "user2", "user2"}).Count(); err != nil {
			t.Error(err)
		} else if count != 2 {
			t.Errorf("count value should `2`, got `%v`", count)
		}
	})
}

func TestCreatByMap(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		if lastId, err := tests.DBEngine.Table("auth_user").Create(map[string]interface{}{
			"username":     "user1",
			"is_superuser": 1,
			"age":          18,
			"date_joined":  time.Now(),
		}); err != nil {
			t.Error(err)
		} else {
			if lastId != 1 {
				t.Errorf("user's primary key should be `1` after create, got : %v", lastId)
			}
			var user tests.AuthUser
			err := tests.DBEngine.Table("auth_user").Where("id=?", lastId).First(&user)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}

			if user.DateJoined.IsZero() {
				t.Errorf("user's data joined should be not zero")
			}

			if user.LastLogin.String != "" {
				t.Errorf("user's last login should be null string")
			}

			if user.UserName != "user1" {
				t.Errorf("user's name should be `user1` after create, got : %v", user.UserName)
			}
		}

		if lastId, err := tests.DBEngine.Table("auth_user").Create(&map[string]interface{}{
			"username":     "user2",
			"is_superuser": 1,
			"age":          18,
			"date_joined":  time.Now(),
			"last_login":   time.Now(),
		}); err != nil {
			t.Error(err)
		} else if lastId != 2 {
			t.Errorf("user's primary key should be `2` after create, got : %v", lastId)
		}

		if _, err := tests.DBEngine.Table("auth_user").Create([]map[string]interface{}{
			{
				"username":     "user5",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
				"last_login":   time.Now(),
			},
		}); err == nil {
			t.Error("create object form map slice should be err")
		}
	})
}

func TestCreateByStruct(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		if lastId, err := tests.DBEngine.Table("auth_user").Create(tests.AuthUser{
			UserName:    "user3",
			IsSuperuser: true,
			Age:         18,
			ModelTime: tests.ModelTime{
				DateJoined: time.Now(),
			},
		}); err != nil {
			t.Error(err)
		} else if lastId != 1 {
			t.Errorf("user's primary key should be `1` after create, got : %v", lastId)
		}

		if lastId, err := tests.DBEngine.Table("auth_user").Create(&tests.AuthUser{
			UserName:    "user4",
			IsSuperuser: true,
			Age:         18,
			ModelTime: tests.ModelTime{
				DateJoined: time.Now(),
			},
		}); err != nil {
			t.Error(err)
		} else if lastId != 2 {
			t.Errorf("user's primary key should be `2` after create, got : %v", lastId)
		}

		if lastId, err := tests.DBEngine.Table("auth_user").Create(&tests.AuthUser{
			UserName: "user4",
			Age:      8,
			ModelTime: tests.ModelTime{
				DateJoined: time.Now(),
				LastLogin: sql.NullString{
					String: "2020-06-03 10:00:00",
					Valid:  true,
				},
			},
		}); err != nil {
			t.Error(err)
		} else if lastId != 3 {
			t.Errorf("user's primary key should be `3` after create, got : %v", lastId)
		}

		if _, err := tests.DBEngine.Table("auth_user").Create([]tests.AuthUser{
			{
				UserName:    "user4",
				IsSuperuser: true,
				ModelTime: tests.ModelTime{
					DateJoined: time.Now(),
				},
			},
		}); err == nil {
			t.Error("create object form struct slice should be err")
		}

	})
}

func TestBulkCreateByMapSlice(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		if lastIdList, err := tests.DBEngine.Table("auth_user").BulkCreate([]map[string]interface{}{
			{
				"username":     "user1",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
				"last_login":   time.Now(),
			},
			{
				"username":     "user2",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
				"last_login":   time.Now(),
			},
			{
				"username":     "user3",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
				"last_login":   time.Now(),
			},
		}); err != nil {
			t.Error(err)
		} else {
			var lastId int64
			dataLen := 3
			err := tests.DBEngine.Table("auth_user").Select("id").Desc("id").First(&lastId)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if lastIdList[dataLen-1] != lastId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", lastId, lastIdList[dataLen-1])
			}
			firstId := lastId - int64(dataLen) + 1
			if lastIdList[0] != firstId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", firstId, lastIdList[0])
			}
		}

		if lastIdList, err := tests.DBEngine.Table("auth_user").BulkCreate(&[]map[string]interface{}{
			{
				"username":     "user4",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
			},
			{
				"username":     "user5",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
			},
		}); err != nil {
			t.Error(err)
		} else {
			var lastId int64
			dataLen := 2
			err := tests.DBEngine.Table("auth_user").Select("id").Desc("id").First(&lastId)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if lastIdList[dataLen-1] != lastId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", lastId, lastIdList[dataLen-1])
			}
			firstId := lastId - int64(dataLen) + 1
			if lastIdList[0] != firstId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", firstId, lastIdList[0])
			}
		}

		if lastIdList, err := tests.DBEngine.Table("auth_user").BulkCreate([]*map[string]interface{}{
			{
				"username":     "user4",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
			},
			{
				"username":     "user5",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
			},
		}); err != nil {
			t.Error(err)
		} else {
			var lastId int64
			dataLen := 2
			err := tests.DBEngine.Table("auth_user").Select("id").Desc("id").First(&lastId)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if lastIdList[dataLen-1] != lastId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", lastId, lastIdList[dataLen-1])
			}
			firstId := lastId - int64(dataLen) + 1
			if lastIdList[0] != firstId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", firstId, lastIdList[0])
			}
		}

		if lastIdList, err := tests.DBEngine.Table("auth_user").BulkCreate(&[]*map[string]interface{}{
			{
				"username":     "user4",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
			},
			{
				"username":     "user5",
				"is_superuser": 1,
				"age":          18,
				"date_joined":  time.Now(),
			},
		}); err != nil {
			t.Error(err)
		} else {
			var lastId int64
			dataLen := 2
			err := tests.DBEngine.Table("auth_user").Select("id").Desc("id").First(&lastId)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if lastIdList[dataLen-1] != lastId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", lastId, lastIdList[dataLen-1])
			}
			firstId := lastId - int64(dataLen) + 1
			if lastIdList[0] != firstId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", firstId, lastIdList[0])
			}
		}

		if _, err := tests.DBEngine.Table("auth_user").BulkCreate(map[string]interface{}{
			"username":     "user1",
			"is_superuser": 1,
			"age":          18,
			"date_joined":  time.Now(),
		}); err == nil {
			t.Error("bulk create object form map should be err")
		}
		if _, err := tests.DBEngine.Table("auth_user").BulkCreate([]map[string]interface{}{}); err == nil {
			t.Error("bulk create object form empty map slice should be err")
		}
	})
}

func TestBulkCreateByStructSlice(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		if lastIdList, err := tests.DBEngine.Table("auth_user").BulkCreate([]tests.AuthUser{
			{
				UserName:    "user4",
				IsSuperuser: true,
				Age:         18,
				ModelTime: tests.ModelTime{
					DateJoined: time.Now(),
				},
			},
			{
				UserName:    "user4",
				IsSuperuser: true,
				Age:         18,
				ModelTime: tests.ModelTime{
					DateJoined: time.Now(),
				},
			},
		}); err != nil {
			t.Error(err)
		} else {
			var lastId int64
			err := tests.DBEngine.Table("auth_user").Select("id").Desc("id").First(&lastId)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if lastIdList[1] != lastId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", lastId, lastIdList[1])
			}
		}

		if lastIdList, err := tests.DBEngine.Table("auth_user").BulkCreate(&[]tests.AuthUser{
			{
				UserName:    "user4",
				IsSuperuser: true,
				Age:         18,
				ModelTime: tests.ModelTime{
					DateJoined: time.Now(),
				},
			},
			{
				UserName:    "user4",
				IsSuperuser: true,
				Age:         18,
				ModelTime: tests.ModelTime{
					DateJoined: time.Now(),
				},
			},
		}); err != nil {
			t.Error(err)
		} else {
			var lastId int64
			dataLen := 2
			err := tests.DBEngine.Table("auth_user").Select("id").Desc("id").First(&lastId)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if lastIdList[dataLen-1] != lastId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", lastId, lastIdList[dataLen-1])
			}
			firstId := lastId - int64(dataLen) + 1
			if lastIdList[0] != firstId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", firstId, lastIdList[0])
			}
		}

		if lastIdList, err := tests.DBEngine.Table("auth_user").BulkCreate([]*tests.AuthUser{
			{
				UserName:    "user4",
				IsSuperuser: true,
				Age:         18,
				ModelTime: tests.ModelTime{
					DateJoined: time.Now(),
				},
			},
		}); err != nil {
			t.Error(err)
		} else {
			var lastId int64
			err := tests.DBEngine.Table("auth_user").Select("id").Desc("id").First(&lastId)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if lastIdList[0] != lastId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", lastId, lastIdList[0])
			}
		}

		if lastIdList, err := tests.DBEngine.Table("auth_user").BulkCreate(&[]*tests.AuthUser{
			{
				UserName:    "user4",
				IsSuperuser: true,
				Age:         18,
				ModelTime: tests.ModelTime{
					DateJoined: time.Now(),
				},
			},
		}); err != nil {
			t.Error(err)
		} else {
			var lastId int64
			err := tests.DBEngine.Table("auth_user").Select("id").Desc("id").First(&lastId)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if lastIdList[0] != lastId {
				t.Errorf("user's primary key should be `%v` after bulk create, got : %v", lastId, lastIdList[0])
			}
		}

		if _, err := tests.DBEngine.Table("auth_user").BulkCreate(tests.AuthUser{
			UserName:    "user4",
			IsSuperuser: true,
			Age:         18,
			ModelTime: tests.ModelTime{
				DateJoined: time.Now(),
			},
		}); err == nil {
			t.Error("bulk create object form struct should be err")
		}
		if _, err := tests.DBEngine.Table("auth_user").BulkCreate([]tests.AuthUser{}); err == nil {
			t.Error("bulk create object form empty struct slice should be err")
		}

	})
}

func TestDelete(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 8)
		if rows, err := tests.DBEngine.Table("auth_user").Where("id in ?", []int{1, 2, 3}).Delete(); err != nil {
			t.Error(err)
		} else if rows != 3 {
			t.Errorf("rows affected should be `3` after delete, got %v", rows)
		}

		if rows, err := tests.DBEngine.Table("auth_user").Where("id > ?", 8).Delete(); err != nil {
			t.Error(err)
		} else if rows != 0 {
			t.Errorf("rows affected should be `0` after delete, got %v", rows)
		}

		if _, err := tests.DBEngine.Table("auth_user").Delete(); err == nil || !errors.Is(err, sqldb.ErrMissingWhereClause) {
			t.Errorf("should returns `sqldb.ErrMissingWhereClause` error, got `%v`", err)
		}
	})
}

func TestUpdate(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 8)
		if affected, err := tests.DBEngine.Table("auth_user").Where("id in ?", []int{1, 3}).Update("username", "user1-3"); err != nil {
			t.Error(err)
		} else {
			if affected != 2 {
				t.Errorf("rows affected should be `2` after update, got %v", affected)
			}
			var names []string
			err := tests.DBEngine.Table("auth_user").Select("username").Where("id in ?", []int{1, 3}).Find(&names)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if names[0] != "user1-3" || names[1] != "user1-3" {
				t.Errorf("name should be `[test1-3, test1-3]` after update, got %v", names)
			}
		}

		if affected, err := tests.DBEngine.Table("auth_user").Where("id = ?", 9).Update("username", "user1-3"); err != nil {
			t.Error(err)
		} else if affected != 0 {
			t.Errorf("rows affected should be `0` after update, got %v", affected)
		}
	})
}

func TestBulkUpdate(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 8)
		nowTime := time.Now()
		if affected, err := tests.DBEngine.Table("auth_user").Where("id in ?", []int{1, 3}).BulkUpdate(map[string]interface{}{
			"username":   "user1-3",
			"last_login": nowTime,
		}); err != nil {
			t.Error(err)
		} else {
			if affected != 2 {
				t.Errorf("rows affected should be `2` after bulk update, got %v", affected)
			}

			var users []tests.AuthUser
			err := tests.DBEngine.Table("auth_user").Where("id in ?", []int{1, 3}).Find(&users)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if users[1].UserName != "user1-3" {
				t.Errorf("name should be `test1-3` after bulk update, got %v", users[1].UserName)
			}
			lastLogin, _ := time.Parse("2006-01-02T15:04:05Z", users[0].LastLogin.String)
			if nowTime.Equal(lastLogin) {
				t.Errorf("success time should be `%v` after bulk update, got %v", nowTime, lastLogin)
			}
		}

		if affected, err := tests.DBEngine.Table("auth_user").Where("id = ?", 9).BulkUpdate(
			map[string]interface{}{
				"username": "user1-3",
				"age":      1,
			},
		); err != nil {
			t.Error(err)
		} else if affected != 0 {
			t.Errorf("rows affected should be `0` after bulk update, got %v", affected)
		}
	})
}

func TestFindScanMapSlice(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 7)
		var mapping []map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(2).Find(&mapping); err != nil {
			t.Error(err)
		} else {
			if len(mapping) != 2 {
				t.Errorf("find value count should `2`, got `%v`", len(mapping))
			}
			for idx, m := range mapping {
				var userId int64
				if id, ok := m["id"].(string); ok {
					userId, _ = strconv.ParseInt(id, 0, 64)
				} else if id, ok := m["id"].(int64); ok {
					userId = id
				}

				if idx == 0 && userId != 1 {
					t.Errorf("first id value should `1`, got %v", userId)
				}
				if idx == 1 && userId != 2 {
					t.Errorf("two id value should `2`, got %v", userId)
				}
			}
		}

		var mapping1 []map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(2).Find(mapping1); err == nil {
			t.Fatal("query should be `must pass a pointer, not a value, to scan destination` error")
		}

		var mapping2 []*map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(2).Find(&mapping2); err != nil {
			t.Error(err)
		} else {
			if len(mapping2) != 2 {
				t.Errorf("find value count should 2, got %v", len(mapping2))
			}

			for idx, m := range mapping2 {
				var userId int64
				if id, ok := (*m)["id"].(string); ok {
					userId, _ = strconv.ParseInt(id, 0, 64)
				} else if id, ok := (*m)["id"].(int64); ok {
					userId = id
				}

				if idx == 0 && userId != 1 {
					t.Errorf("first id value should `1`, got %v", userId)
				}
				if idx == 1 && userId != 2 {
					t.Errorf("two id value should `2`, got `%v`", userId)
				}
			}
		}

		mapping3 := make([]map[string]interface{}, 0)
		if err := tests.DBEngine.Table("auth_user").Offset(2).Limit(2).Find(&mapping3); err != nil {
			t.Errorf("map dest find query error, got %v", err)
		} else {
			if len(mapping3) != 2 {
				t.Errorf("find value count should 2, got %v", len(mapping3))
			}

			for idx, m := range mapping3 {
				var userId int64
				if id, ok := m["id"].(string); ok {
					userId, _ = strconv.ParseInt(id, 0, 64)
				} else if id, ok := m["id"].(int64); ok {
					userId = id
				}

				if idx == 0 && userId != 3 {
					t.Errorf("first id value should `30`, got `%v`", userId)
				}
				if idx == 1 && userId != 4 {
					t.Errorf("two id value should `4`, got `%v`", userId)
				}
			}
		}

		mapping4 := make([]map[string]interface{}, 0)
		if err := tests.DBEngine.Table("auth_user").Offset(2).Limit(2).Find(mapping4); err == nil {
			t.Fatal("query should be `must pass a pointer, not a value, to scan destination` error")
		}

		mapping5 := make([]*map[string]interface{}, 0)
		if err := tests.DBEngine.Table("auth_user").Offset(2).Limit(2).Find(mapping5); err == nil {
			t.Fatal("query should be `must pass a pointer, not a value, to scan destination` error")
		}
	})
}

func TestFindScanStructSlice(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 7)
		var users []tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(2).Find(&users); err != nil {
			t.Error(err)
		} else {
			if len(users) != 2 {
				t.Errorf("find value count should 2, got %v", len(users))
			}

			for idx, u := range users {
				if idx == 0 && u.Id != 1 {
					t.Errorf("first id value should `1`, got %v", u.Id)
				}
				if idx == 1 && u.Id != 2 {
					t.Errorf("two id value should `2`, got %v", u.Id)
				}
			}
		}

		var users1 []*tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Where("id in ?", []int{3, 4}).Find(&users1); err != nil {
			t.Error(err)
		} else {
			if len(users1) != 2 {
				t.Errorf("find value count should 2, got %v", len(users1))
			}

			for idx, u := range users1 {
				if idx == 0 && (*u).Id != 3 {
					t.Errorf("first id value should `3`, got %v", u.Id)
				}
				if idx == 1 && (*u).Id != 4 {
					t.Errorf("two id value should `4`, got %v", u.Id)
				}
				if !u.DateJoined.IsZero() {
					t.Errorf("user's updated at should be zero, but got %v", u.DateJoined)
				}
			}
		}

		users2 := make([]tests.SimpleAuthUser, 0)
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(3).Offset(2).Find(&users2); err != nil {
			t.Error(err)
		} else {
			if len(users2) != 3 {
				t.Errorf("find value count should 3, got %v", len(users2))
			}

			for idx, u := range users2 {
				if idx == 0 && u.Id != 3 {
					t.Errorf("first id value should `3`, got %v", u.Id)
				}
				if idx == 2 && u.Id != 5 {
					t.Errorf("three id value should `5`, got `%v`", u.Id)
				}
			}
		}

		var users3 []tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(2).Find(users3); err == nil {
			t.Fatal("query should be `must pass a pointer, not a value, to scan destination` error")
		}

		var users4 []*tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(2).Find(users4); err == nil {
			t.Fatal("query should be `must pass a pointer, not a value, to scan destination` error")
		}

		users5 := make([]tests.SimpleAuthUser, 0)
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(2).Find(users5); err == nil {
			t.Fatal("query should be `must pass a pointer, not a value, to scan destination` error")
		}
	})
}

func TestFirstScanMap(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 3, 4, 5, 6, 7)
		var user = map[string]interface{}{}
		if err := tests.DBEngine.Table("auth_user").Select("id, username").First(&user); err != nil {
			t.Error(err)
		} else {
			var userId int64
			if id, ok := user["id"].(string); ok {
				userId, _ = strconv.ParseInt(id, 0, 64)
			} else if id, ok := user["id"].(int64); ok {
				userId = id
			}
			if userId != 1 {
				t.Errorf("user id value should be `1`, got %v", userId)
			}
		}

		var user1 = map[string]interface{}{}
		if err := tests.DBEngine.Table("auth_user").Where("id =?", 4).First(user1); err != nil {
			t.Error(err)
		} else {
			var userId int64
			if id, ok := user1["id"].(string); ok {
				userId, _ = strconv.ParseInt(id, 0, 64)
			} else if id, ok := user1["id"].(int64); ok {
				userId = id
			}

			if userId != 4 {
				t.Errorf("user id value should be `4`, got %v", userId)
			}
		}

		var user2 = &map[string]interface{}{}
		if err := tests.DBEngine.Table("auth_user").Where("id=?", 3).First(user2); err != nil {
			t.Error(err)
		} else {
			var userId int64
			if id, ok := (*user2)["id"].(string); ok {
				userId, _ = strconv.ParseInt(id, 0, 64)
			} else if id, ok := (*user2)["id"].(int64); ok {
				userId = id
			}

			if userId != 3 {
				t.Errorf("user id value should be `3`, got %v", userId)
			}
		}

		var user3 = &map[string]interface{}{}
		if err := tests.DBEngine.Table("auth_user").Where("id=?", 4).First(&user3); err != nil {
			t.Error(err)
		} else {
			var userId int64
			if id, ok := (*user3)["id"].(string); ok {
				userId, _ = strconv.ParseInt(id, 0, 64)
			} else if id, ok := (*user3)["id"].(int64); ok {
				userId = id
			}

			if userId != 4 {
				t.Errorf("user id value should be `4`, got %v", userId)
			}
		}

		var notFound = map[string]interface{}{}
		if err := tests.DBEngine.Table("auth_user").Where("id=?", 2).First(&notFound); err == nil || !errors.Is(err, sqldb.ErrRecordNotFound) {
			t.Errorf("error should be `sqldb.ErrRecordNotFound`, got `%v`", err)
		}

		var nilDest map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").First(&nilDest); err == nil {
			t.Errorf("error should be nil pointer passed to scan destination")
		} else {
			t.Log(err)
		}

		var nilPtrDest *map[string]interface{}
		if err := tests.DBEngine.Table("auth_user").First(&nilPtrDest); err == nil {
			t.Errorf("error should be nil pointer passed to scan destination")
		} else {
			t.Log(err)
		}
	})

}

func TestFirstScanStruct(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 3, 4, 5, 6, 7)
		var user tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").First(&user); err != nil {
			t.Error(err)
		} else if user.Id != 1 {
			t.Errorf("user id value should be `1`, got %v", user.Id)
		}

		var user1 tests.AuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Where("id=?", 3).First(&user1); err != nil {
			t.Error(err)
		} else {
			if user1.UserName != "user3" {
				t.Errorf("user name value should be `user3`, got %v", user1.UserName)
			}

			if !user1.DateJoined.IsZero() {
				t.Errorf("user update at value should be null, got %v", user1.DateJoined)
			}
		}

		var user2 *tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id", "username").Where("id=?", 3).First(&user2); err != nil {
			t.Error(err)
		} else if (*user2).Id != 3 {
			t.Errorf("user id value should be `3`, got %v", (*user2).Id)
		}

		var user3 = tests.SimpleAuthUser{}
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Where("id = ?", 4).First(&user3); err != nil {
			t.Error(err)
		} else if user3.Id != 4 {
			t.Errorf("user id value should be `4`, got %v", user3.Id)
		}

		var user4 = &tests.SimpleAuthUser{}
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Where("id = ?", 4).First(user4); err != nil {
			t.Error(err)
		} else if user4.UserName != "user4" {
			t.Errorf("user name value should be `test4`, got %v", user4.UserName)
		}

		var user5 = &tests.SimpleAuthUser{}
		if err := tests.DBEngine.Table("auth_user").Select("id, username").Where("id = ?", 4).First(&user5); err != nil {
			t.Error(err)
		} else if user5.Id != 4 {
			t.Errorf("user id value should be `4`, got %v", user5.Id)
		}

		var user6 *tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id", "username").Where("id=?", 3).First(user6); err == nil {
			t.Errorf("error should be nil pointer passed to scan destination")
		}

		var user7 tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Select("id", "username").Where("id=?", 3).First(user7); err == nil {
			t.Errorf("error should be must pass a pointer, not a value, to StructScan destination")
		}

		var notFound tests.SimpleAuthUser
		if err := tests.DBEngine.Table("auth_user").Where("id = ?", 12).First(&notFound); err == nil || !errors.Is(err, sqldb.ErrRecordNotFound) {
			t.Errorf("error should be `sqldb.ErrRecordNotFound`, got `%v`", err)
		}
	})
}

func TestFirstScanPluckDest(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 3, 4)
		var count int
		if err := tests.DBEngine.Table("auth_user").Select("count(*)").Where("id in ?", []int{1, 3, 4, 2}).First(&count); err != nil {
			t.Error(err)
		} else if count != 3 {
			t.Errorf("query count should be `3`, got %v", count)
		}
	})
}

func TestFindScanPluckDest(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 3, 4)
		var userIds []int
		if err := tests.DBEngine.Table("auth_user").Select("id").Find(&userIds); err != nil {
			t.Error(err)
		} else if len(userIds) != 3 {
			t.Errorf("query count should be `3`, got %v", len(userIds))
		}

		var userNames []string
		if err := tests.DBEngine.Table("auth_user").Select("username").Where("id in ?", []int{3, 4}).Find(&userNames); err != nil {
			t.Error(err)
		} else if len(userNames) != 2 {
			t.Errorf("query count should be `2`, got %v", len(userNames))
		}
	})
}

func TestQueryWithSession(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2, 3, 4, 5, 6, 7)
		rows, err := tests.DBEngine.Table("auth_user").Select("id, username").Limit(2).Query()
		if err != nil {
			t.Error(err)
		}
		defer rows.Close()
		var users []tests.SimpleAuthUser
		for rows.Next() {
			user := tests.SimpleAuthUser{}
			if err := rows.StructScan(&user); err != nil {
				t.Error(err)
			}
			users = append(users, user)
		}
		if len(users) != 2 {
			t.Errorf("find value count should 2, got %v", len(users))
		}

		for idx, u := range users {
			if idx == 0 && u.Id != 1 {
				t.Errorf("first id value should `1`, got %v", u.Id)
			}
			if idx == 1 && u.Id != 2 {
				t.Errorf("two id value should `2`, got %v", u.Id)
			}
		}
	})
}

func TestQueryRowWithSession(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(3)
		row := tests.DBEngine.Table("auth_user").Limit(1).QueryRow()
		var user = tests.AuthUser{}
		err := row.StructScan(&user)
		if err != nil {
			t.Error(err)
		}
		if user.Id != 3 {
			t.Error("wrapper QueryRowx error")
		}
	})
}

func TestExpr(t *testing.T) {
	tests.RunWithDB(t, func(t *testing.T) {
		tests.InsertAuthUserWithId(1, 2)
		if affected, err := tests.DBEngine.Table("auth_user").Where("id=?", 1).BulkUpdate(
			map[string]interface{}{"age": sqldb.Expr("age + ?", 3)},
		); err != nil {
			t.Error(err)
		} else {
			if affected != 1 {
				t.Errorf("rows affected should be `1` after update, got %v", affected)
			}

			var age uint
			err := tests.DBEngine.Table("auth_user").Select("age").Where("id =?", 1).First(&age)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if age != 21 {
				t.Errorf("age should be `21` after update, got %v", age)
			}
		}

		if affected, err := tests.DBEngine.Table("auth_user").Where("id=?", 2).Update("age", sqldb.Expr("age + 20-5")); err != nil {
			t.Error(err)
		} else {
			if affected != 1 {
				t.Errorf("rows affected should be `1` after update, got %v", affected)
			}

			var age uint
			err := tests.DBEngine.Table("auth_user").Select("age").Where("id =?", 2).First(&age)
			if err != nil {
				t.Fatalf("errors happened when query: %v", err)
			}
			if age != 33 {
				t.Errorf("age should be `33` after update, got %v", age)
			}
		}
	})
}
