# URL shortener

URL shortener is an application which provide make short URL and store results into YDB.

## Usage

### Running as http-server

```bash
go get -u github.com/ydb-platform/ydb-go-sdk/v3/example/url_shortener
YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa/key/file \
url_shortener \
   -ydb=grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu \
   -port=80
```
Open http://localhost/ in browse and use URL shortener web interface

### Running as serverless function
Yandex function needs a go module project. First you must create go.mod file.
```bash
go mod init example && go mod tidy
zip archive.zip service.go go.mod go.sum
yc sls fn version create \
   --service-account-id=aje46n285h0re8nmm5u6 \
   --runtime=golang116 \
   --entrypoint=main.Serverless \
   --memory=128m \
   --execution-timeout=1s \
   --environment YDB="grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g8skpblkos03malf3s/etnpa7o3qltdfgu9vsap" \
   --source-path=./archive.zip \
   --function-id=d4euc5gp5614b56crpnj
```
