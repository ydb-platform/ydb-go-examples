package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"log"
	"path"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

const BatchSize = 1000

type Command struct {
	table string
	count int
}

type logMessage struct {
	App       string
	Host      string
	Timestamp time.Time
	HTTPCode  uint32
	Message   string
}

func wrap(err error, explanation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", explanation, err)
	}
	return err
}

func getLogBatch(logs []logMessage, offset int) []logMessage {
	logs = logs[:0]
	for i := 0; i < BatchSize; i++ {
		message := logMessage{
			App:       fmt.Sprintf("App_%d", offset%10),
			Host:      fmt.Sprintf("192.168.0.%d", offset%11),
			Timestamp: time.Now().Add(time.Millisecond * time.Duration(i%1000)),
			HTTPCode:  200,
		}
		if i%2 == 0 {
			message.Message = "GET / HTTP/1.1"
		} else {
			message.Message = "GET /images/logo.png HTTP/1.1"
		}

		logs = append(logs, message)
	}
	return logs
}

func (cmd *Command) createLogTable(ctx context.Context, c table.Client) error {
	log.Printf("Create table: %v\n", cmd.table)
	err := c.Do(
		ctx,
		func(ctx context.Context, session table.Session) error {
			return session.CreateTable(ctx, cmd.table,
				options.WithColumn("App", types.Optional(types.TypeUTF8)),
				options.WithColumn("Timestamp", types.Optional(types.TypeTimestamp)),
				options.WithColumn("Host", types.Optional(types.TypeUTF8)),
				options.WithColumn("HTTPCode", types.Optional(types.TypeUint32)),
				options.WithColumn("Message", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("App", "Timestamp", "Host"),
			)
		},
	)
	return wrap(err, "failed to create table")
}

func (cmd *Command) writeLogBatch(ctx context.Context, c table.Client, logs []logMessage) error {
	err := c.Do(
		ctx,
		func(ctx context.Context, session table.Session) error {
			rows := make([]types.Value, 0, len(logs))

			for _, msg := range logs {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("App", types.UTF8Value(msg.App)),
					types.StructFieldValue("Host", types.UTF8Value(msg.Host)),
					types.StructFieldValue("Timestamp", types.TimestampValueFromTime(msg.Timestamp)),
					types.StructFieldValue("HTTPCode", types.Uint32Value(msg.HTTPCode)),
					types.StructFieldValue("Message", types.UTF8Value(msg.Message)),
				))
			}

			return wrap(session.BulkUpsert(ctx, cmd.table, types.ListValue(rows...)),
				"failed to perform bulk upsert")
		})
	return wrap(err, "failed to write log batch")
}

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

	tableName := cmd.table
	cmd.table = path.Join(params.Prefix(), cmd.table)

	err = sugar.RmPath(ctx, db, params.Prefix(), tableName)
	if err != nil {
		return err
	}
	err = sugar.MakePath(ctx, db, params.Prefix())
	if err != nil {
		return err
	}

	if err := cmd.createLogTable(ctx, db.Table()); err != nil {
		return wrap(err, "failed to create table")
	}
	var logs []logMessage
	for offset := 0; offset < cmd.count; offset++ {
		logs = getLogBatch(logs, offset)
		if err := cmd.writeLogBatch(ctx, db.Table(), logs); err != nil {
			return wrap(err, fmt.Sprintf("failed to write batch offset %d", offset))
		}
		fmt.Print(".")
	}
	fmt.Print("\n")
	log.Print("Done.\n")

	return nil
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.IntVar(&cmd.count, "count", 1000, "count requests")
	flagSet.StringVar(&cmd.table, "table", "bulk_upsert_example", "Path for table")
}
