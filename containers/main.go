package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var (
	dsn       string
	prefix    string
	tablePath string
	count     int
)

func init() {
	required := []string{"ydb", "table"}
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
	flagSet.IntVar(&count,
		"count", 1000,
		"count requests",
	)
	flagSet.StringVar(&tablePath,
		"table", "bulk_upsert_example",
		"Path for table",
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
	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(dsn),
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	err = db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			tx, err := s.BeginTransaction(ctx, table.TxSettings(
				table.WithSerializableReadWrite(),
			))
			if err != nil {
				return err
			}
			defer func() {
				_ = tx.Rollback(context.Background())
			}()

			res, err := tx.Execute(ctx, render(query, nil), nil)
			if err != nil {
				return err
			}
			if _, err = tx.CommitTx(ctx); err != nil {
				return err
			}

			parsers := [...]func(){
				func() {
					_ = res.Scan(&exampleList{})
				},
				func() {
					_ = res.Scan(&exampleTuple{})
				},
				func() {
					_ = res.Scan(&exampleDict{})
				},
				func() {
					_ = res.Scan(&exampleStruct{})
				},
				func() {
					_ = res.Scan(&variantStruct{})
				},
				func() {
					_ = res.Scan(&variantTuple{})
				},
			}

			for set := 0; res.NextResultSet(ctx); set++ {
				res.NextRow()
				parsers[set]()
				if err = res.Err(); err != nil {
					return err
				}
			}
			return res.Err()
		},
	)
	if err != nil {
		panic(err)
	}
}
