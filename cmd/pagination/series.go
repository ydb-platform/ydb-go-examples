package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"path"

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

	err = sugar.RmPath(ctx, db, params.Prefix(), "schools")
	if err != nil {
		return err
	}
	err = sugar.MakePath(ctx, db, params.Prefix())
	if err != nil {
		return err
	}

	err = createTable(ctx, db.Table(), path.Join(params.Prefix(), "schools"))
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}

	err = fillTableWithData(ctx, db.Table(), params.Prefix())
	if err != nil {
		return fmt.Errorf("fill tables with data error: %w", err)
	}

	var lastNum uint
	lastCity := ""
	limit := 3
	maxPages := 10
	for i, empty := 0, false; i < maxPages && !empty; i++ {
		fmt.Printf("> Page %v:\n", i+1)
		empty, err = selectPaging(ctx, db.Table(), params.Prefix(), limit, &lastNum, &lastCity)
		if err != nil {
			return fmt.Errorf("get page %v error: %w", i, err)
		}
	}

	return nil
}

func selectPaging(
	ctx context.Context, c table.Client, prefix string, limit int, lastNum *uint, lastCity *string) (
	empty bool, err error) {

	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $limit AS Uint64;
		DECLARE $lastCity AS Utf8;
		DECLARE $lastNumber AS Uint32;

		$Data = (
			SELECT * FROM schools
			WHERE city = $lastCity AND number > $lastNumber

			UNION ALL

			SELECT * FROM schools
			WHERE city > $lastCity
			ORDER BY city, number LIMIT $limit
		);
		SELECT * FROM $Data ORDER BY city, number LIMIT $limit;`, prefix)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res result.Result
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$limit", types.Uint64Value(uint64(limit))),
					table.ValueParam("$lastCity", types.UTF8Value(*lastCity)),
					table.ValueParam("$lastNumber", types.Uint32Value(uint32(*lastNum))),
				),
			)
			return
		},
	)
	if err != nil {
		return
	}
	if err = res.Err(); err != nil {
		return
	}
	if !res.NextResultSet(ctx, "city", "number", "address") || !res.HasNextRow() {
		empty = true
		return
	}
	var addr string
	for res.NextRow() {
		err = res.ScanWithDefaults(lastCity, lastNum, &addr)
		if err != nil {
			return false, err
		}
		fmt.Printf("\t%v, School #%v, Address: %v\n", *lastCity, *lastNum, addr)
	}
	return
}

func fillTableWithData(ctx context.Context, c table.Client, prefix string) (err error) {
	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $schoolsData AS List<Struct<
			city: Utf8,
			number: Uint32,
			address: Utf8>>;

		REPLACE INTO schools
		SELECT
			city,
			number,
			address
		FROM AS_TABLE($schoolsData);`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$schoolsData", getSchoolData()),
			))
			return err
		})
	return err
}

func createTable(ctx context.Context, c table.Client, path string) (err error) {
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path,
				options.WithColumn("city", types.Optional(types.TypeUTF8)),
				options.WithColumn("number", types.Optional(types.TypeUint32)),
				options.WithColumn("address", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("city", "number"),
			)
		},
	)
	if err != nil {
		return err
	}

	return nil
}
