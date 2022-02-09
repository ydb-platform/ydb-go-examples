all: basic bulk_upsert containers ddl decimal healthcheck pagination partitioning_policies read_table ttl ttl_readtable url_shortener

lint:
	golangci-lint run ./cmd/basic ./cmd/bulk_upsert ./cmd/containers ./cmd/ddl ./cmd/decimal ./cmd/healthcheck ./cmd/pagination ./cmd/partitioning_policies ./cmd/read_table ./cmd/ttl ./cmd/ttl_readtable ./cmd/url_shortener

basic:
	go run ./cmd/basic -ydb=${YDB_CONNECTION_STRING} -prefix=basic

bulk_upsert:
	go run ./cmd/bulk_upsert -ydb=${YDB_CONNECTION_STRING} -prefix=bulk_upsert

containers:
	go run ./cmd/containers -ydb=${YDB_CONNECTION_STRING} -prefix=containers

ddl:
	go run ./cmd/ddl -ydb=${YDB_CONNECTION_STRING} -prefix=ddl

decimal:
	go run ./cmd/decimal -ydb=${YDB_CONNECTION_STRING} -prefix=decimal

healthcheck:
	go run ./cmd/healthcheck -ydb=${YDB_CONNECTION_STRING} -prefix=healthcheck ya.ru google.com

pagination:
	go run ./cmd/pagination -ydb=${YDB_CONNECTION_STRING} -prefix=pagination

partitioning_policies:
	go run ./cmd/partitioning_policies -ydb=${YDB_CONNECTION_STRING} -prefix=partitioning_policies

read_table:
	go run ./cmd/read_table -ydb=${YDB_CONNECTION_STRING} -prefix=read_table

ttl:
	go run ./cmd/ttl -ydb=${YDB_CONNECTION_STRING} -prefix=ttl

ttl_readtable:
	go run ./cmd/ttl_readtable -ydb=${YDB_CONNECTION_STRING} -prefix=ttl_readtable

url_shortener:
	go run ./cmd/url_shortener -ydb=${YDB_CONNECTION_STRING} -prefix=url_shortener
