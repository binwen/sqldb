export Driver="mysql"
export Dns="root:@/sqldb?charset=utf8&parseTime=True"
export Slaves="root:@/sqldb?charset=utf8&parseTime=True;root:@/sqldb?charset=utf8&parseTime=True"
export ShowSQL=true
export MaxConns=5
export MaxIdleConns=3
export MaxLifetime=4
go test ./...