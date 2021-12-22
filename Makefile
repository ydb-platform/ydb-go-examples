all: basic bulk_upsert containers ddl decimal healthcheck pagination partitioning_policies read_table ttl ttl_readtable url_shortener

basic:
	go run ./cmd/basic -ydb=${DSN} -prefix=basic

bulk_upsert:
	go run ./cmd/bulk_upsert -ydb=${DSN} -prefix=bulk_upsert

containers:
	go run ./cmd/containers -ydb=${DSN} -prefix=containers

ddl:
	go run ./cmd/ddl -ydb=${DSN} -prefix=ddl

decimal:
	go run ./cmd/decimal -ydb=${DSN} -prefix=decimal

healthcheck:
	go run ./cmd/healthcheck -ydb=${DSN} -prefix=healthcheck

pagination:
	go run ./cmd/pagination -ydb=${DSN} -prefix=pagination

partitioning_policies:
	go run ./cmd/partitioning_policies -ydb=${DSN} -prefix=partitioning_policies

read_table:
	go run ./cmd/read_table -ydb=${DSN} -prefix=read_table

ttl:
	go run ./cmd/ttl -ydb=${DSN} -prefix=ttl

ttl_readtable:
	go run ./cmd/ttl_readtable -ydb=${DSN} -prefix=ttl_readtable

url_shortener:
	go run ./cmd/url_shortener -ydb=${DSN} -prefix=url_shortener
