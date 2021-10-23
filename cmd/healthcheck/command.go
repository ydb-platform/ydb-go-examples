package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

type Command struct {
	urls string
}

func (cmd *Command) ExportFlags(ctx context.Context, flagSet *flag.FlagSet) {
	flagSet.StringVar(&cmd.urls, "urls", "", "URLs for check")
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) (err error) {
	s, err := newService(
		ctx,
		ydb.WithConnectParams(params.ConnectParams),
	)
	if err != nil {
		return fmt.Errorf("error on create service: %w", err)
	}
	fmt.Println(params.Args)
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
