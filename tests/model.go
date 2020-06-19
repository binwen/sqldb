package tests

import (
	"database/sql"
	"time"
)

type ModelTime struct {
	DateJoined time.Time      `db:"date_joined"`
	LastLogin  sql.NullString `db:"last_login"`
}

type AuthUser struct {
	ModelTime
	Id          int
	UserName    string `db:"username"`
	Age         int
	IsSuperuser bool `db:"is_superuser"`
}

type AuthGroup struct {
	Id   int
	Name string
}

type AuthUserGroups struct {
	Id      int
	UserId  int `db:"user_id"`
	GroupId int `db:"group_id"`
}

type SimpleAuthUser struct {
	Id       int
	UserName string
}
