package main

import (
	"context"
	"flag"
	"fmt"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

type Command struct {
}

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

	err = prepareScheme(ctx, db, params.Prefix())
	if err != nil {
		return fmt.Errorf("error on prepare scheme: %w", err)
	}

	err = prepareData(ctx, db, params.Prefix())
	if err != nil {
		return fmt.Errorf("error on prepare data: %w", err)
	}

	// Prepare transaction control for upcoming query execution.
	// NOTE: result of TxControl() may be reused.
	txc := table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)

	return db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			// Execute text query without preparation and with given "autocommit"
			// transaction control. That is, transaction will be committed without
			// additional calls. Notice the "_" unused variable – it stands for created
			// transaction during execution, but as said above, transaction is committed
			// for us, and we do not want to do anything with it.
			_, res, err := s.Execute(ctx, txc,
				`
			DECLARE $mystr AS Utf8?;

			SELECT 42 as id, $mystr as mystr;
		`,
				table.NewQueryParameters(
					table.ValueParam("$mystr", types.OptionalValue(types.UTF8Value("test"))),
				),
			)
			if err != nil {
				return err // handle error
			}
			// Scan for received values within the result set(s).
			// res.Err() reports the reason of last unsuccessful one.
			var (
				id    int32
				myStr *string //optional value
			)
			for res.NextResultSet(ctx, "id", "mystr") {
				for res.NextRow() {
					// Suppose our "users" table has two rows: id and age.
					// Thus, current row will contain two appropriate items with
					// exactly the same order.
					err := res.Scan(&id, &myStr)

					// Error handling.
					if err != nil {
						return err
					}
					// do something with data
					fmt.Printf("got id %v, got mystr: %v\n", id, *myStr)
				}
			}
			return res.Err()
		},
	)
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}
