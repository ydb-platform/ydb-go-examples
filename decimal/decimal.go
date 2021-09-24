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

	"github.com/ydb-platform/ydb-go-examples/pkg/cli"
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
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := ydb.New(
		connectCtx,
		params.ConnectParams,
		environ.WithEnvironCredentials(ctx),
	)

	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = db.Close() }()

	session, err := db.Table().CreateSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = session.Close(context.Background())
	}()

	var (
		tablePathPrefix = path.Join(params.Database(), params.Prefix())
		tablePath       = path.Join(tablePathPrefix, "decimals")
	)
	err = session.CreateTable(ctx, tablePath,
		options.WithColumn("id", types.Optional(types.TypeUint32)),
		options.WithColumn("value", types.Optional(types.DefaultDecimal)),
		options.WithPrimaryKeyColumn("id"),
	)
	if err != nil {
		return err
	}

	write, err := session.Prepare(ctx, render(writeQuery, templateConfig{
		TablePathPrefix: tablePathPrefix,
	}))
	if err != nil {
		return err
	}
	read, err := session.Prepare(ctx, render(readQuery, templateConfig{
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
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
