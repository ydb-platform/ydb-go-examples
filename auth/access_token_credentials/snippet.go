package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/ydb-go-examples/pkg/cli"
)

type Command struct {
	accessToken string
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := ydb.New(
		connectCtx,
		params.ConnectParams,
		ydb.WithAccessTokenCredentials(cmd.accessToken),
	)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = db.Close() }()

	// work with db instance

	return nil
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.StringVar(&cmd.accessToken, "ydb-access-token", "", "access token for YDB authenticate")
}
