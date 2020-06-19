package tests

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

var (
	mysql = map[string]string{
		"auth_user": `
CREATE TABLE auth_user (
  id int(11) NOT NULL AUTO_INCREMENT,
  is_superuser tinyint(1) NOT NULL,
  username varchar(150) NOT NULL,
  age int(11) NOT NULL,
  date_joined datetime NOT NULL,
  last_login datetime DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,

		"auth_group": `
CREATE TABLE auth_group (
  id int(11) NOT NULL AUTO_INCREMENT,
  name varchar(80) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`,
		"auth_user_groups": `
CREATE TABLE auth_user_groups (
  id int(11) NOT NULL AUTO_INCREMENT,
  user_id int(11) NOT NULL,
  group_id int(11) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`,
	}

	postgres = map[string]string{
		"auth_user": `
CREATE TABLE "public"."auth_user" (
  "id" serial PRIMARY KEY,
  "is_superuser" bool NOT NULL,
  "username" varchar(30)  NOT NULL,
  age int4 NOT NULL,
  "last_login" timestamptz(6),
  "date_joined" timestamptz(6) NOT NULL
)`,
		"auth_group": `
CREATE TABLE "public"."auth_group" (
  "id" serial PRIMARY KEY,
  "name" varchar(80) NOT NULL
)
`,
		"auth_user_groups": `
CREATE TABLE "public"."auth_user_groups" (
  "id" serial PRIMARY KEY,
  "user_id" int4 NOT NULL,
  "group_id" int4 NOT NULL
)
`,
	}

	sqlite = map[string]string{
		"auth_user": `
CREATE TABLE auth_user (
  id INTEGER PRIMARY KEY   AUTOINCREMENT,
  is_superuser tinyint(1) NOT NULL,
  username varchar(150) NOT NULL,
  age int(11) NOT NULL,
  date_joined datetime NOT NULL,
  last_login datetime DEFAULT NULL
)`,

		"auth_group": `
CREATE TABLE auth_group (
  id INTEGER PRIMARY KEY   AUTOINCREMENT,
  name varchar(80) NOT NULL
) ;
`,
		"auth_user_groups": `
CREATE TABLE auth_user_groups (
  id INTEGER PRIMARY KEY   AUTOINCREMENT,
  user_id int(11) NOT NULL,
  group_id int(11) NOT NULL
) ;
`,
	}
)

func RunWithDB(t *testing.T, test func(t *testing.T)) {
	db := DBEngine.Use()
	var sqlContext map[string]string
	if db.DriverName() == "postgres" {
		sqlContext = postgres
	} else if db.DriverName() == "sqlite3" {
		sqlContext = sqlite
	} else {
		sqlContext = mysql
	}
	defer func() {
		// for k := range createTableSQL {
		// 	_, err := DBEngine.Raw(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", k)).Exec()
		// 	if err != nil {
		// 		t.Error(err)
		// 	}
		// }
	}()

	for k, v := range sqlContext {
		_, err := db.Raw(fmt.Sprintf("DROP TABLE IF EXISTS %s", k)).Exec()
		if err != nil {
			t.Error(err)
		}

		_, err = db.Raw(v).Exec()
		if err != nil {
			t.Fatalf("create table %s error:%s", k, err)
		}
	}

	test(t)
}

func InsertAuthUserWithId(ids ...int) {
	for _, id := range ids {
		_, err = DBEngine.Raw(
			"insert into auth_user(id, is_superuser,username, age, date_joined,last_login) values(?,?,?,?,?,?)",
			id, 1, "user"+strconv.Itoa(id), 18, time.Now(), time.Now(),
		).Exec()
	}
}

func InsertAuthUserWithName(names ...string) {
	for _, name := range names {
		_, err = DBEngine.Raw(
			"insert into auth_user(is_superuser,username,age,date_joined) values(?,?,?,?)",
			1, name, 18, time.Now(),
		).Exec()
	}
}

func InsertAuthGroup(ids ...int) {
	for _, id := range ids {
		_, err = DBEngine.Raw("insert into auth_group(id,name) values(?,?)", id, "group"+strconv.Itoa(id)).Exec()
	}
}

func InsertUserGroup(userId int, groupId int) {
	_, err = DBEngine.Raw("insert into auth_user_groups(user_id,group_id) values(?,?)", userId, groupId).Exec()
}
