package main

import (
	"context"
	"flag"
	"fmt"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	yc "github.com/ydb-platform/ydb-go-yc"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

type Command struct {
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	db, err := ydb.New(
		ctx,
		ydb.WithConnectParams(params.ConnectParams),
		yc.WithMetadataCredentials(ctx),
	)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = db.Close(ctx) }()

	whoAmI, err := db.Discovery().WhoAmI(ctx)
	if err != nil {
		return err
	}

	fmt.Println(whoAmI.String())

	return nil
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}
