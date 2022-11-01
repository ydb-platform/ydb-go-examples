package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const defaultConnectionString = "grpc://localhost:2136/local"

var (
	host                = flag.String("listen-host", "localhost", "host/ip for start listener")
	port                = flag.Int("port", 3619, "port to listen")
	cacheTimeout        = flag.Duration("cache", time.Second*10, "cache timeout, 0 mean disable cache")
	disableCDC          = flag.Bool("disable-cdc", false, "disable cdc")
	skipCreateTable     = flag.Bool("skip-init", false, "skip recreate table and topic")
	ydbConnectionString = flag.String("ydb-connection-string", "", "ydb connection string, default "+defaultConnectionString)
	ydbToken            = flag.String("ydb-token", "", "Auth token for ydb")
	backendCount        = flag.Int("backend-count", 1, "count of backend servers")
)

func main() {
	flag.Parse()

	ctx := context.Background()
	db := connect()

	if !*skipCreateTable {
		createTableAndCDC(ctx, db, *backendCount)
	}

	servers := make([]http.Handler, *backendCount)
	for i := 0; i < *backendCount; i++ {
		servers[i] = newServer(i, db, *cacheTimeout)
	}
	log.Printf("servers count: %v", len(servers))
	handler := newBalancer(servers...)

	addr := *host + ":" + strconv.Itoa(*port)
	log.Printf("Start listen http://%s\n", addr)
	err := http.ListenAndServe(addr, handler)
	if errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("failed to listen and serve: %+v", err)
	}
}

func connect() ydb.Connection {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if *ydbConnectionString != "" {
		connectionString = *ydbConnectionString
	}
	if connectionString == "" {
		connectionString = defaultConnectionString
	}

	token := os.Getenv("YDB_TOKEN")
	if *ydbToken != "" {
		token = *ydbToken
	}
	var ydbOptions []ydb.Option
	if token != "" {
		ydbOptions = append(ydbOptions, ydb.WithAccessTokenCredentials(token))
	}

	if *ydbToken != "" {
		ydbOptions = append(ydbOptions, ydb.WithAccessTokenCredentials(*ydbToken))
	}
	db, err := ydb.Open(ctx, connectionString, ydbOptions...)
	if err != nil {
		log.Fatalf("failed to create to ydb: %+v", err)
	}
	log.Printf("connected to database")
	return db
}
