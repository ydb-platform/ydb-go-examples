package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	port                = flag.Int("port", 3619, "port to listen")
	cacheTimeout        = flag.Duration("cache-timeout", time.Second*15, "cache timeout")
	enableCDC           = flag.Bool("cdc", true, "enable cdc")
	cdcLoadOnStart      = flag.Duration("cdc-load-time", time.Second, "Load cdc history on start")
	ydbConnectionString = flag.String("ydb-connection-string", "grpc://localhost:2136/local", "ydb connection string")
)

func main() {
	ctx := context.Background()
	db, err := ydb.Open(ctx, *ydbConnectionString)
	if err != nil {
		log.Fatalf("failed to create to ydb: %+v", err)
	}
	log.Printf("connected to database")

	err = createTables(ctx, db)
	if err != nil {
		log.Fatalf("failed to create tables: %+v", err)
	}

	handler := newBalancer(
		newServer(1, db, *cacheTimeout),
		//newServer(2, db, *cacheTimeout),
	)
	err = http.ListenAndServe(":"+strconv.Itoa(*port), handler)
	if errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("failed to listen and serve: %+v", err)
	}
}
