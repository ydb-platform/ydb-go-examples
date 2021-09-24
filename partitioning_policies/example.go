package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"path"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-examples/pkg/cli"
)

type Command struct {
	table string
}

func wrap(err error, explanation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", explanation, err)
	}
	return err
}

func (cmd *Command) testUniformPartitions(ctx context.Context, c table.Client) error {
	log.Printf("Create table: %v\n", cmd.table)

	err, _ := c.Retry(ctx, false, func(ctx context.Context, session table.Session) error {
		err := session.CreateTable(ctx, cmd.table,
			options.WithColumn("key", types.Optional(types.TypeUint64)),
			options.WithColumn("value", types.Optional(types.TypeJSON)),
			options.WithPrimaryKeyColumn("key"),

			options.WithProfile(
				options.WithPartitioningPolicy(
					options.WithPartitioningPolicyMode(options.PartitioningAutoSplitMerge),
					options.WithPartitioningPolicyUniformPartitions(4),
				),
			),
		)
		if err != nil {
			return wrap(err, "failed to create table")
		}

		desc, err := session.DescribeTable(ctx, cmd.table, options.WithShardKeyBounds())
		if err != nil {
			return wrap(err, "failed to get table description")
		}
		if len(desc.KeyRanges) != 4 {
			return errors.New("key ranges len is not as expected")
		}

		return nil
	})
	return wrap(err, "failed to execute operation")
}

func (cmd *Command) testExplicitPartitions(ctx context.Context, c table.Client) error {
	log.Printf("Create table: %v\n", cmd.table)

	err, _ := c.Retry(ctx, false, func(ctx context.Context, session table.Session) error {
		err := session.CreateTable(ctx, cmd.table,
			options.WithColumn("key", types.Optional(types.TypeUint64)),
			options.WithColumn("value", types.Optional(types.TypeJSON)),
			options.WithPrimaryKeyColumn("key"),

			options.WithProfile(
				options.WithPartitioningPolicy(
					options.WithPartitioningPolicyExplicitPartitions(
						types.TupleValue(types.OptionalValue(types.Uint64Value(100))),
						types.TupleValue(types.OptionalValue(types.Uint64Value(300))),
						types.TupleValue(types.OptionalValue(types.Uint64Value(400))),
					),
				),
			),
		)
		if err != nil {
			return wrap(err, "failed to create table")
		}

		desc, err := session.DescribeTable(ctx, cmd.table, options.WithShardKeyBounds())
		if err != nil {
			return wrap(err, "failed to get table description")
		}
		if len(desc.KeyRanges) != 4 {
			return errors.New("key ranges len is not as expected")
		}

		return nil
	})
	return wrap(err, "failed to execute operation")
}

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

	tableName := cmd.table
	cmd.table = path.Join(params.Prefix(), cmd.table)

	err = db.Scheme().CleanupDatabase(ctx, params.Prefix(), tableName)
	if err != nil {
		return err
	}
	err = db.Scheme().EnsurePathExists(ctx, params.Prefix())
	if err != nil {
		return err
	}

	if err = cmd.testUniformPartitions(ctx, db.Table()); err != nil {
		return wrap(err, "failed to test uniform partitions")
	}

	err = db.Scheme().CleanupDatabase(ctx, params.Prefix(), tableName)
	if err != nil {
		return err
	}

	if err := cmd.testExplicitPartitions(ctx, db.Table()); err != nil {
		return wrap(err, "failed to test explicit partitions")
	}

	return nil
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.StringVar(&cmd.table, "table", "explicit_partitions_example", "Path for table")
}
