package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

type Command struct {
	port int
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	s, err := newService(ctx, ydb.WithConnectParams(params.ConnectParams))
	if err != nil {
		return fmt.Errorf("error on create service: %w", err)
	}
	defer s.Close(ctx)
	return http.ListenAndServe(":"+strconv.Itoa(cmd.port), http.HandlerFunc(s.Router))
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.IntVar(&cmd.port, "port", 80, "http port for web-server")
}
