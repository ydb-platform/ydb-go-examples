package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

type Command struct {
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	db, err := ydb.New(
		ctx,
		ydb.WithConnectParams(params.ConnectParams),
		environ.WithEnvironCredentials(ctx),
	)

	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = db.Close(ctx) }()

	tableName := "orders"
	fmt.Println("Read whole table, unsorted:")
	err = readTable(ctx, db.Table(), path.Join(params.Prefix(), tableName))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Sorted by composite primary key:")
	err = readTable(ctx, db.Table(), path.Join(params.Prefix(), tableName), options.ReadOrdered())
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Any five rows:")
	err = readTable(ctx, db.Table(), path.Join(params.Prefix(), tableName), options.ReadRowLimit(5))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("First five rows by PK (ascending) with subset of columns:")
	err = readTable(ctx, db.Table(), path.Join(params.Prefix(), tableName), options.ReadRowLimit(5), options.ReadColumn("customer_id"),
		options.ReadColumn("order_id"),
		options.ReadColumn("order_date"), options.ReadOrdered())
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with first PK component (customer_id,) greater or equal than 2 and less then 3:")
	keyRange := options.KeyRange{
		From: types.TupleValue(types.OptionalValue(types.Uint64Value(2))),
		To:   types.TupleValue(types.OptionalValue(types.Uint64Value(3))),
	}
	err = readTable(ctx, db.Table(), path.Join(params.Prefix(), tableName), options.ReadKeyRange(keyRange))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with composite PK lexicographically less or equal than (1,4):")
	err = readTable(ctx, db.Table(), path.Join(params.Prefix(), tableName), options.ReadLessOrEqual(types.TupleValue(types.OptionalValue(types.Uint64Value(1)), types.OptionalValue(types.Uint64Value(4)))))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with composite PK lexicographically greater or equal than (1,2) and less than (3,4):")
	keyRange = options.KeyRange{
		From: types.TupleValue(types.OptionalValue(types.Uint64Value(1)), types.OptionalValue(types.Uint64Value(2))),
		To:   types.TupleValue(types.OptionalValue(types.Uint64Value(3)), types.OptionalValue(types.Uint64Value(1))),
	}
	err = readTable(ctx, db.Table(), path.Join(params.Prefix(), tableName), options.ReadKeyRange(keyRange))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	return nil
}

type row struct {
	id          uint64
	orderID     uint64
	date        time.Time
	description string
}

func readTable(ctx context.Context, c table.Client, path string, opts ...options.ReadTableOption) (err error) {
	var res result.StreamResult

	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path, opts...)
			return err
		},
	)
	if err != nil {
		return err
	}

	r := row{}
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			if res.CurrentResultSet().ColumnCount() == 4 {
				err = res.ScanWithDefaults(&r.id, &r.orderID, &r.description, &r.date)
				if err != nil {
					return err
				}
				log.Printf("#  Order, CustomerId: %d, OrderId: %d, Description: %s, Order date: %s", r.id, r.orderID, r.description, r.date.Format("2006-01-02"))
			} else {
				err = res.ScanWithDefaults(&r.id, &r.orderID, &r.date)
				if err != nil {
					return err
				}
				log.Printf("#  Order, CustomerId: %d, OrderId: %d, Order date: %s", r.id, r.orderID, r.date.Format("2006-01-02"))
			}
		}
	}
	return res.Err()
}
