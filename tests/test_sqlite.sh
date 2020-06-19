export Driver="sqlite3"
export Dns="./sqldb.db?cache=shared&mode=rwc"
export Slaves=""
export ShowSQL=false
export MaxConns=5
export MaxIdleConns=3
export MaxLifetime=4
go test  ./...