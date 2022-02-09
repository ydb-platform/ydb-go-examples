package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	dsn    string
	prefix string
	urls   = URLs{}
)

// URLs is a flag.Value implementation which holds URL's as string slice
type URLs struct {
	urls []string
}

// String returns string representation of URLs
func (u *URLs) String() string {
	return fmt.Sprintf("%v", u.urls)
}

// Set appends new value to URLs holder
func (u *URLs) Set(s string) error {
	u.urls = append(u.urls, s)
	return nil
}

func init() {
	required := []string{"ydb", "url"}
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options] -url=URL1 [-url=URL2 -url=URL3]\n", os.Args[0])
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
	flagSet.Var(&urls,
		"url",
		"url for health check",
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newService(
		ctx,
		ydb.WithConnectionString(dsn),
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("error on create service: %w", err))
	}
	defer s.Close(ctx)
	for {
		if err := s.check(ctx, urls.urls); err != nil {
			panic(fmt.Errorf("error on check URLS %v: %w", urls, err))
		}
		select {
		case <-time.After(time.Minute):
			continue
		case <-ctx.Done():
			return
		}
	}
}

// Serverless is an entrypoint for serverless yandex function
// nolint:deadcode
func Serverless(ctx context.Context) error {
	s, err := newService(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB")),
		environ.WithEnvironCredentials(ctx),
		ydb.WithDialTimeout(time.Second),
	)
	if err != nil {
		return fmt.Errorf("error on create service: %w", err)
	}
	defer s.Close(ctx)
	return s.check(ctx, strings.Split(os.Getenv("URLS"), ","))
}
