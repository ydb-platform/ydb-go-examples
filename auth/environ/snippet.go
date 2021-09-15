package main

import (
	"context"
	"flag"
	"fmt"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3/connect"

	"github.com/ydb-platform/ydb-go-examples/pkg/cli"
)

type Command struct {
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(
		connectCtx,
		params.ConnectParams,
		environ.WithEnvironCredentials(ctx),
		// No need to specify other credentials options for use authenticate from environment variables
	)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = db.Close() }()

	// work with db instance

	return nil
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}
