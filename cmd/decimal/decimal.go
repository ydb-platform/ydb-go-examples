package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/big"
	"path"
	"text/template"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

type templateConfig struct {
	TablePathPrefix string
}

var writeQuery = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

DECLARE $decimals AS List<Struct<
	id: Uint32,
	value: Decimal(22,9)>>;

REPLACE INTO decimals
SELECT
	id,
	value
FROM AS_TABLE($decimals);
`))

var readQuery = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
SELECT value FROM decimals;
`))

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

	var (
		tablePathPrefix = path.Join(params.Database(), params.Prefix())
		tablePath       = path.Join(tablePathPrefix, "decimals")
	)
	err = db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, tablePath,
				options.WithColumn("id", types.Optional(types.TypeUint32)),
				options.WithColumn("value", types.Optional(types.DefaultDecimal)),
				options.WithPrimaryKeyColumn("id"),
			)
		},
	)
	if err != nil {
		return err
	}

	return db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			write, err := s.Prepare(ctx, render(writeQuery, templateConfig{
				TablePathPrefix: tablePathPrefix,
			}))
			if err != nil {
				return err
			}

			read, err := s.Prepare(ctx, render(readQuery, templateConfig{
				TablePathPrefix: tablePathPrefix,
			}))
			if err != nil {
				return err
			}

			txc := table.TxControl(
				table.BeginTx(
					table.WithSerializableReadWrite(),
				),
				table.CommitTx(),
			)

			x := big.NewInt(42 * 1000000000)
			x.Mul(x, big.NewInt(2))

			_, _, err = write.Execute(ctx, txc, table.NewQueryParameters(
				table.ValueParam("$decimals",
					types.ListValue(
						types.StructValue(
							types.StructFieldValue("id", types.Uint32Value(42)),
							types.StructFieldValue("value", types.DecimalValueFromBigInt(x, 22, 9)),
						),
					),
				),
			))
			if err != nil {
				return err
			}

			_, res, err := read.Execute(ctx, txc, nil)
			if err != nil {
				return err
			}
			var p *types.Decimal
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.Scan(&p)
					if err != nil {
						return err
					}

					fmt.Println(p.String())
				}
			}
			return res.Err()
		},
	)
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
