package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

type command struct {
	urls string
}

func (cmd *command) ExportFlags(ctx context.Context, flagSet *flag.FlagSet) {
	flagSet.StringVar(&cmd.urls, "urls", "", "URLs for check")
}

func (cmd *command) Run(ctx context.Context, params cli.Parameters) (err error) {
	s, err := newService(
		ctx,
		ydb.WithConnectParams(params.ConnectParams),
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		return fmt.Errorf("error on create service: %w", err)
	}
	defer s.Close(ctx)
	for {
		if err := s.check(ctx, params.Args); err != nil {
			return fmt.Errorf("error on check URLS [%v]: %w", params.Args, err)
		}
		select {
		case <-time.After(time.Minute):
			continue
		case <-ctx.Done():
			return nil
		}
	}
}
