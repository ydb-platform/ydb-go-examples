package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	dsn           string
	prefix        string
	port          int
	shutdownAfter time.Duration
)

func init() {
	required := []string{"ydb"}
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&dsn,
		"ydb", "",
		"YDB connection string",
	)
	flagSet.StringVar(&prefix,
		"prefix", "",
		"tables prefix",
	)
	flagSet.IntVar(&port,
		"port", 80,
		"http port for web-server",
	)
	flagSet.DurationVar(&shutdownAfter,
		"shutdown-after", -1,
		"duration for shutdown after start",
	)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		flagSet.Usage()
		os.Exit(1)
	}
	flagSet.Visit(func(f *flag.Flag) {
		for i, arg := range required {
			if arg == f.Name {
				required = append(required[:i], required[i+1:]...)
			}
		}
	})
	if len(required) > 0 {
		fmt.Printf("\nSome required options not defined: %v\n\n", required)
		flagSet.Usage()
		os.Exit(1)
	}
}

func main() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		done   = make(chan struct{})
	)
	if shutdownAfter > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), shutdownAfter)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	s, err := newService(ctx, ydb.WithConnectionString(dsn))
	if err != nil {
		panic(fmt.Errorf("error on create service: %w", err))
	}
	defer s.Close(ctx)

	server := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: http.HandlerFunc(s.Router),
	}
	defer func() {
		_ = server.Shutdown(ctx)
	}()

	go func() {
		_ = server.ListenAndServe()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return
	case <-done:
		return
	}
}

// Serverless is an entrypoint for serverless yandex function
// nolint:deadcode
func Serverless(w http.ResponseWriter, r *http.Request) {
	s, err := newService(
		r.Context(),
		ydb.WithConnectionString(os.Getenv("YDB")),
		environ.WithEnvironCredentials(r.Context()),
	)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer s.Close(r.Context())
	s.Router(w, r)
}
