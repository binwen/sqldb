export Driver="postgres"
export Dns="host=10.0.3.3 port=5432 user=postgres dbname=sqldb sslmode=disable password=xtx123"
export Slaves="host=10.0.3.3 port=5432 user=postgres dbname=sqldb sslmode=disable password=xtx123;host=10.0.3.3 port=5432 user=postgres dbname=sqldb sslmode=disable password=xtx123"
export ShowSQL=false
export MaxConns=5
export MaxIdleConns=3
export MaxLifetime=4
go test  ./...