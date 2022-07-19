package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicalter"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	dsn    string
	prefix string
)

func init() {
	required := []string{"ydb"}
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&dsn,
		"ydb", "",
		"YDB connection string",
	)
	flagSet.StringVar(&prefix,
		"prefix", "",
		"tables prefix",
	)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		flagSet.Usage()
		os.Exit(1)
	}
	flagSet.Visit(func(f *flag.Flag) {
		for i, arg := range required {
			if arg == f.Name {
				required = append(required[:i], required[i+1:]...)
			}
		}
	})
	if len(required) > 0 {
		fmt.Printf("\nSome required options not defined: %v\n\n", required)
		flagSet.Usage()
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := ydb.Open(
		ctx,
		dsn,
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix = path.Join(db.Name(), prefix)

	tableName := "cdc"

	log.Println("Drop table (if exists)...")
	err = dropTableIfExists(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
	)
	if err != nil {
		panic(fmt.Errorf("drop table error: %w", err))
	}
	log.Println("Drop table done")

	log.Println("Create table...")
	err = createTable(
		ctx,
		db.Table(),
		prefix, tableName,
	)
	if err != nil {
		panic(fmt.Errorf("create table error: %w", err))
	}
	log.Println("Create table done")

	go fillTable(ctx, db.Table(), prefix, tableName)
	go func() {
		time.Sleep(interval / 2)
		cleanTable(ctx, db.Table(), prefix, tableName)
	}()

	topicPath := tableName + "/feed"
	consumerName := "test-consumer"
	log.Println("Create consumer")
	err = db.Topic().AlterTopic(ctx, topicPath, topicalter.AddConsumers(topictypes.Consumer{
		Name: consumerName,
	}))
	if err != nil {
		panic(fmt.Errorf("failed to create feed consumer", err))
	}

	log.Println("Start cdc read")
	reader, err := db.Topic().StartReader(consumerName, []topicreader.ReadSelector{{Path: topicPath}})
	if err != nil {
		log.Fatal("failed to start read feed", err)
	}

	for {
		mess, err := reader.ReadMessage(ctx)
		if err != nil {
			panic(fmt.Errorf("failed to read message", err))
		}

		var event interface{}
		err = mess.UnmarshalTo(sugar.JSONUnmarshaler(&event))
		if err != nil {
			panic(fmt.Errorf("failed to unmarshal json cdc", err))
		}
		log.Println("new cdc event:", event)
	}
}
