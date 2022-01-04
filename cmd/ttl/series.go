package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"math/rand"
	"path"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

const (
	DocTablePartitionCount = 4
	ExpirationQueueCount   = 4
)

type Command struct {
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

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

	cleanupDBs := []string{"documents"}
	for i := 0; i < ExpirationQueueCount; i++ {
		cleanupDBs = append(cleanupDBs, fmt.Sprintf("expiration_queue_%v", i))
	}

	err = sugar.RemoveRecursive(ctx, db, params.Prefix())
	if err != nil {
		return err
	}
	err = sugar.MakeRecursive(ctx, db, params.Prefix())
	if err != nil {
		return err
	}

	err = createTables(ctx, db.Table(), params.Prefix())
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}

	err = addDocument(ctx, db.Table(), params.Prefix(),
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		1)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = addDocument(ctx, db.Table(), params.Prefix(),
		"https://ya.ru/",
		"<html><body><h1>Ya</h1></body></html>",
		2)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}
	err = readDocument(ctx, db.Table(), params.Prefix(), "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	for i := uint64(0); i < ExpirationQueueCount; i++ {
		if err = deleteExpired(ctx, db.Table(), params.Prefix(), i, 1); err != nil {
			return fmt.Errorf("delete expired failed: %w", err)
		}
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = addDocument(ctx, db.Table(), params.Prefix(),
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		2)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = addDocument(ctx, db.Table(), params.Prefix(),
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		3)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	for i := uint64(0); i < ExpirationQueueCount; i++ {
		if err = deleteExpired(ctx, db.Table(), params.Prefix(), i, 2); err != nil {
			return fmt.Errorf("delete expired failed: %w", err)
		}
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}
	err = readDocument(ctx, db.Table(), params.Prefix(), "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	return nil
}

func readExpiredBatchTransaction(ctx context.Context, c table.Client, prefix string, queue,
	timestamp, prevTimestamp, prevDocID uint64) (result.Result,
	error) {

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $timestamp AS Uint64;
        DECLARE $prev_timestamp AS Uint64;
        DECLARE $prev_doc_id AS Uint64;

        $data = (
            SELECT *
            FROM expiration_queue_%v
            WHERE
                ts <= $timestamp
                AND
                ts > $prev_timestamp

            UNION ALL

            SELECT *
            FROM expiration_queue_%v
            WHERE
                ts = $prev_timestamp AND doc_id > $prev_doc_id
            ORDER BY ts, doc_id
            LIMIT 100
        );

        SELECT ts, doc_id
        FROM $data
        ORDER BY ts, doc_id
        LIMIT 100;`, prefix, queue, queue)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res result.Result
	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, readTx, table.NewQueryParameters(
				table.ValueParam("$timestamp", types.Uint64Value(timestamp)),
				table.ValueParam("$prev_timestamp", types.Uint64Value(prevTimestamp)),
				table.ValueParam("$prev_doc_id", types.Uint64Value(prevDocID)),
			))
			return err
		},
	)
	if err != nil {
		return nil, err
	}
	if res.Err() != nil {
		return nil, res.Err()
	}
	return res, nil
}

func deleteDocumentWithTimestamp(ctx context.Context, c table.Client, prefix string, queue, lastDocID, timestamp uint64) error {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $doc_id AS Uint64;
        DECLARE $timestamp AS Uint64;

        DELETE FROM documents
        WHERE doc_id = $doc_id AND ts = $timestamp;

        DELETE FROM expiration_queue_%v
        WHERE ts = $timestamp AND doc_id = $doc_id;`, prefix, queue)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$doc_id", types.Uint64Value(lastDocID)),
				table.ValueParam("$timestamp", types.Uint64Value(timestamp)),
			))
			return err
		},
	)
	return err
}

func deleteExpired(ctx context.Context, c table.Client, prefix string, queue, timestamp uint64) error {
	fmt.Printf("> DeleteExpired from queue #%d:\n", queue)
	empty := false
	lastTimestamp := uint64(0)
	lastDocID := uint64(0)

	for !empty {
		res, err := readExpiredBatchTransaction(ctx, c, prefix, queue, timestamp, lastTimestamp, lastDocID)
		if err != nil {
			return err
		}

		empty = true
		res.NextResultSet(ctx)
		for res.NextRow() {
			empty = false
			err = res.ScanWithDefaults(&lastDocID, &lastTimestamp)
			if err != nil {
				return err
			}
			fmt.Printf("\tDocId: %d\n\tTimestamp: %d\n", lastDocID, lastTimestamp)

			err = deleteDocumentWithTimestamp(ctx, c, prefix, queue, lastDocID, lastTimestamp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func readDocument(ctx context.Context, c table.Client, prefix, url string) error {
	fmt.Printf("> ReadDocument \"%v\":\n", url)

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

        DECLARE $url AS Utf8;

        $doc_id = Digest::CityHash($url);

        SELECT doc_id, url, html, ts
        FROM documents
        WHERE doc_id = $doc_id;`, prefix)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res result.Result
	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, readTx, table.NewQueryParameters(
				table.ValueParam("$url", types.UTF8Value(url)),
			))
			return err
		},
	)
	if err != nil {
		return err
	}
	if res.Err() != nil {
		return res.Err()
	}
	var (
		docID  *uint64
		docURL *string
		ts     *uint64
		html   *string
	)
	if res.NextResultSet(ctx, "doc_id", "url", "ts", "html") && res.NextRow() {
		err = res.Scan(&docID, &docURL, &ts, &html)
		if err != nil {
			return err
		}
		fmt.Printf("\tDocId: %v\n", docID)
		fmt.Printf("\tUrl: %v\n", docURL)
		fmt.Printf("\tTimestamp: %v\n", ts)
		fmt.Printf("\tHtml: %v\n", html)
	} else {
		fmt.Println("\tNot found")
	}

	return nil
}

func addDocument(ctx context.Context, c table.Client, prefix, url, html string, timestamp uint64) error {
	fmt.Printf("> AddDocument: \n\tUrl: %v\n\tTimestamp: %v\n", url, timestamp)

	queue := rand.Intn(ExpirationQueueCount)
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $url AS Utf8;
        DECLARE $html AS Utf8;
        DECLARE $timestamp AS Uint64;

        $doc_id = Digest::CityHash($url);

        REPLACE INTO documents
            (doc_id, url, html, ts)
        VALUES
            ($doc_id, $url, $html, $timestamp);

        REPLACE INTO expiration_queue_%v
            (ts, doc_id)
        VALUES
            ($timestamp, $doc_id);`, prefix, queue)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$url", types.UTF8Value(url)),
				table.ValueParam("$html", types.UTF8Value(html)),
				table.ValueParam("$timestamp", types.Uint64Value(timestamp)),
			))
			return err
		},
	)
	return err
}

func createTables(ctx context.Context, c table.Client, prefix string) (err error) {
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "documents"),
				options.WithColumn("doc_id", types.Optional(types.TypeUint64)),
				options.WithColumn("url", types.Optional(types.TypeUTF8)),
				options.WithColumn("html", types.Optional(types.TypeUTF8)),
				options.WithColumn("ts", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("doc_id"),
				options.WithProfile(
					options.WithPartitioningPolicy(
						options.WithPartitioningPolicyUniformPartitions(uint64(DocTablePartitionCount)))),
			)
		},
	)
	if err != nil {
		return err
	}

	for i := 0; i < ExpirationQueueCount; i++ {
		err = c.Do(
			ctx,
			func(ctx context.Context, s table.Session) error {
				return s.CreateTable(ctx, path.Join(prefix, fmt.Sprintf("expiration_queue_%v", i)),
					options.WithColumn("doc_id", types.Optional(types.TypeUint64)),
					options.WithColumn("ts", types.Optional(types.TypeUint64)),
					options.WithPrimaryKeyColumn("ts", "doc_id"),
				)
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}
