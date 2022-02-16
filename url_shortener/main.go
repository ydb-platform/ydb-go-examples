package main

import (
	"context"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbMetrics "github.com/ydb-platform/ydb-go-sdk-prometheus"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
)

var (
	dsn           string
	prefix        string
	port          int
	shutdownAfter time.Duration
	logLevel      string

	log *zap.Logger
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
	flagSet.StringVar(&logLevel,
		"log-level", "INFO",
		"logging level",
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

	var err error
	log, err = zap.NewDevelopment(
		zap.IncreaseLevel(
			func() zapcore.Level {
				for l := zapcore.DebugLevel; l < zapcore.FatalLevel; l++ {
					if l.CapitalString() == strings.ToUpper(logLevel) {
						return l
					}
				}
				return zapcore.InfoLevel
			}(),
		),
	)
	if err != nil {
		panic(err)
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

	s, err := newService(
		ctx,
		ydb.WithConnectionString(dsn),
		ydbMetrics.WithTraces(
			prometheus.NewRegistry(),
			ydbMetrics.WithSeparator("_"),
			ydbMetrics.WithDetails(
				trace.DetailsAll,
			),
		),
		ydbZap.WithTraces(
			log,
			trace.DetailsAll,
		),
	)
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
