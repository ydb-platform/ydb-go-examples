package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"os"
	"path"
	"strconv"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

var (
	dsn    string
	prefix string
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

func createTable(ctx context.Context, c table.Client, prefix string) (err error) {
	return c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "series"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeUint64)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
	)
}

func tableVersion(ctx context.Context, c table.Client, prefix string) (version int, _ error) {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			description, err := s.DescribeTable(ctx, path.Join(prefix, "series"))
			if err != nil {
				if ydb.IsOperationErrorSchemeError(err) {
					version = -1
					return nil
				}
				return err
			}
			if v, has := description.Attributes["version"]; has {
				version, err = strconv.Atoi(v)
				if err != nil {
					return err
				}
			}
			return nil
		},
	)
	return version, err
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := []ydb.Option{
		environ.WithEnvironCredentials(ctx),
	}
	db, err := ydb.Open(ctx, dsn, opts...)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix = path.Join(db.Name(), prefix)

	err = sugar.RemoveRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	err = sugar.MakeRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	err = describeTableOptions(ctx, db.Table())
	if err != nil {
		panic(fmt.Errorf("describe table options error: %w", err))
	}

	err = createTables(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = describeTable(ctx, db.Table(), path.Join(
		prefix, "series",
	))
	if err != nil {
		panic(fmt.Errorf("describe table error: %w", err))
	}

	err = fillTablesWithData(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}

	err = selectSimple(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("select simple error: %w", err))
	}

	err = scanQuerySelect(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("scan query select error: %w", err))
	}

	err = readTable(ctx, db.Table(), path.Join(
		prefix, "series",
	))
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}
}
