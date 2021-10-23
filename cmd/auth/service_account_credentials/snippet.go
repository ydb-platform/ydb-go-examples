package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-yc"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

type Command struct {
	serviceAccountKeyFile string
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	db, err := ydb.New(
		ctx,
		ydb.WithConnectParams(params.ConnectParams),
		yc.WithServiceAccountKeyFileCredentials(cmd.serviceAccountKeyFile),
	)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = db.Close(ctx) }()

	// work with db instance

	return nil
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.StringVar(&cmd.serviceAccountKeyFile, "service-account-key-file", "", "service account key file for YDB authenticate")
}
