# ydb-go-examples

> examples for work with YDB 

## Navigation of examples

Example | Description | Run command
--- | --- | ---
`cmd/auth/access_token_credentials` | authenticate with access token credentials | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/access_token_credentials#readme)
`cmd/auth/anonymous_credentials` | authenticate with anonymous credentials | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/anonymous_credentials#readme)
`cmd/auth/metadata_credentials` | authenticate with metadata credentials | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/metadata_credentials#readme)
`cmd/auth/service_account_credentials` | authenticate with service account credentials | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/service_account_credentials#readme)
`cmd/auth/environ` | authenticate using environment variables | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/environ#readme)
`cmd/basic` | store and read the series  | `make basic`
`cmd/bulk_upsert` | bulk upserting data | `make bulk_upsert`
`cmd/containers` | containers example | `make containers`
`cmd/ddl` | DDL requests example | `make ddl`
`cmd/decimal` | decimal store and read | `make decimal`
`cmd/healthcheck` | healthcheck site by URL (yandex function and local http-server) | `make healthcheck`
`cmd/pagination` | pagination example | `make pagination`
`cmd/partitioning_policies` | partitioning_policies example | `make partitioning_policies`
`cmd/read_table` | read table example | `make read_table`
`cmd/ttl` | TTL using example | `make ttl`
`cmd/ttl_readtable` | TTL using example | `make ttl_readtable`
`cmd/url_shortener` | URL shortener example (yandex function and local http-server) | `make url_shortener`

Run command needs prepared environ like this:
```bash
export YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=~/.ydb/SA.json
export DSN="grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn02qhd0tfkrq4riqgd"
```
