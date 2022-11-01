package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"strconv"
)

const defaultConnectionString = "grpc://localhost:2136/local"

var (
	host                = flag.String("listen-host", "localhost", "host/ip for start listener")
	port                = flag.Int("port", 3619, "port to listen")
	cacheTimeout        = flag.Duration("cache", 0, "cache timeout, 0 mean disable cache")
	ydbConnectionString = flag.String("ydb-connection-string", "", "ydb connection string, default "+defaultConnectionString)
	ydbToken            = flag.String("ydb-token", "", "Auth token for ydb")
)

func main() {
	flag.Parse()

	db := connect()

	handler := newBalancer(
		newServer(0, db, *cacheTimeout),
		// newServer(1, db, *cacheTimeout),
	)

	addr := *host + ":" + strconv.Itoa(*port)
	log.Printf("Start listen http://%s\n", addr)
	err := http.ListenAndServe(addr, handler)
	if errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("failed to listen and serve: %+v", err)
	}
}
