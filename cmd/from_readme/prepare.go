package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"path"
	"text/template"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type templateConfig struct {
	TablePathPrefix string
}

var fill = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
DECLARE $ordersData AS List<Struct<
	customer_id: Uint64,
	order_id: Uint64,
	description: Utf8,
	order_date: Date>>;
REPLACE INTO orders
SELECT
	customer_id,
	order_id,
	description,
    order_date
FROM AS_TABLE($ordersData);
`))

func prepareScheme(ctx context.Context, db ydb.Connection, prefix string) (err error) {
	err = sugar.RmPath(ctx, db, prefix, "orders")
	if err != nil {
		return err
	}
	err = sugar.MakePath(ctx, db, prefix)
	if err != nil {
		return err
	}

	err = createTables(ctx, db.Table(), prefix)
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}
	return nil
}

func prepareData(ctx context.Context, db ydb.Connection, prefix string) (err error) {
	err = fillTablesWithData(ctx, db.Table(), prefix)
	if err != nil {
		return fmt.Errorf("fill tables with data error: %w", err)
	}
	return nil
}

func createTables(ctx context.Context, c table.Client, prefix string) (err error) {
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "orders"),
				options.WithColumn("customer_id", types.Optional(types.TypeUint64)),
				options.WithColumn("order_id", types.Optional(types.TypeUint64)),
				options.WithColumn("description", types.Optional(types.TypeUTF8)),
				options.WithColumn("order_date", types.Optional(types.TypeDate)),
				options.WithPrimaryKeyColumn("customer_id", "order_id"),
				//For sorting demonstration
				options.WithProfile(
					options.WithPartitioningPolicy(
						options.WithPartitioningPolicyExplicitPartitions(
							types.TupleValue(types.OptionalValue(types.Uint64Value(1))),
							types.TupleValue(types.OptionalValue(types.Uint64Value(2))),
							types.TupleValue(types.OptionalValue(types.Uint64Value(3))),
						),
					),
				),
			)
		},
	)
	if err != nil {
		return err
	}
	return nil
}

func fillTablesWithData(ctx context.Context, c table.Client, prefix string) (err error) {
	// Prepare write transaction.
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, render(fill, templateConfig{
				TablePathPrefix: prefix,
			}))
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$ordersData", getSeasonsData()),
			))
			return err
		})
	if err != nil {
		return err
	}
	return err
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func seasonData(customerID, orderID uint64, description string, date time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("customer_id", types.Uint64Value(customerID)),
		types.StructFieldValue("order_id", types.Uint64Value(orderID)),
		types.StructFieldValue("description", types.UTF8Value(description)),
		types.StructFieldValue("order_date", types.DateValueFromTime(date)),
	)
}

func getSeasonsData() types.Value {
	return types.ListValue(
		seasonData(1, 1, "Order 1", days("2006-02-03")),
		seasonData(1, 2, "Order 2", days("2007-08-24")),
		seasonData(1, 3, "Order 3", days("2008-11-21")),
		seasonData(1, 4, "Order 4", days("2010-06-25")),
		seasonData(2, 1, "Order 1", days("2014-04-06")),
		seasonData(2, 2, "Order 2", days("2015-04-12")),
		seasonData(2, 3, "Order 3", days("2016-04-24")),
		seasonData(2, 4, "Order 4", days("2017-04-23")),
		seasonData(2, 5, "Order 5", days("2018-03-25")),
		seasonData(3, 1, "Order 1", days("2019-04-23")),
		seasonData(3, 2, "Order 3", days("2020-03-25")),
	)
}

const dateISO8601 = "2006-01-02"

func days(date string) time.Time {
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}
