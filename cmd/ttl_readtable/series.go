package main

import (
	"context"
	"flag"
	"fmt"
	"path"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-examples/internal/cli"
)

const (
	docTablePartitionCount = 4
	deleteBatchSize        = 10
)

type command struct {
}

func (cmd *command) ExportFlags(context.Context, *flag.FlagSet) {}

func (cmd *command) Run(ctx context.Context, params cli.Parameters) error {
	db, err := ydb.New(
		ctx,
		ydb.WithConnectParams(params.ConnectParams),
		environ.WithEnvironCredentials(ctx),
	)

	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = db.Close(ctx) }()

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

	err = addDocument(ctx, db.Table(), params.Prefix(),
		"https://mail.yandex.ru/",
		"<html><body><h1>Mail</h1></body></html>",
		3)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = addDocument(ctx, db.Table(), params.Prefix(),
		"https://zen.yandex.ru/",
		"<html><body><h1>Zen</h1></body></html>",
		4)
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

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://mail.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://zen.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = deleteExpired(ctx, db.Table(), params.Prefix(), 2)
	if err != nil {
		return fmt.Errorf("delete expired failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://mail.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://zen.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = addDocument(ctx, db.Table(), params.Prefix(),
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		3)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = addDocument(ctx, db.Table(), params.Prefix(),
		"https://ya.ru/",
		"<html><body><h1>Ya</h1></body></html>",
		4)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = deleteExpired(ctx, db.Table(), params.Prefix(), 3)
	if err != nil {
		return fmt.Errorf("delete expired failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://mail.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = readDocument(ctx, db.Table(), params.Prefix(), "https://zen.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	return nil
}

func deleteExpiredDocuments(ctx context.Context, c table.Client, prefix string, ids []uint64,
	timestamp uint64) error {
	fmt.Printf("> DeleteExpiredDocuments: %+v\n", ids)

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $keys AS List<Struct<
            doc_id: Uint64
        >>;

        DECLARE $timestamp AS Uint64;

        $expired = (
            SELECT d.doc_id AS doc_id
            FROM AS_TABLE($keys) AS k
            INNER JOIN documents AS d
            ON k.doc_id = d.doc_id
            WHERE ts <= $timestamp
        );

        DELETE FROM documents ON
        SELECT * FROM $expired;`, prefix)

	keys := types.ListValue(func() []types.Value {
		var k = make([]types.Value, len(ids))
		for i := range ids {
			k[i] = types.StructValue(types.StructFieldValue("doc_id", types.Uint64Value(ids[i])))
		}
		return k
	}()...)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				table.NewQueryParameters(
					table.ValueParam("$keys", keys),
					table.ValueParam("$timestamp", types.Uint64Value(timestamp)),
				),
				options.WithQueryCachePolicy(
					options.WithQueryCachePolicyKeepInCache()))
			return err
		},
	)
	return err
}

func deleteExpiredRange(ctx context.Context, c table.Client, prefix string, timestamp uint64,
	keyRange options.KeyRange) error {
	fmt.Printf("> DeleteExpiredRange: %+v\n", keyRange)

	var res result.StreamResult
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path.Join(prefix, "documents"),
				options.ReadKeyRange(keyRange),
				options.ReadColumn("doc_id"),
				options.ReadColumn("ts"))
			return err
		},
	)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	if err = res.Err(); err != nil {
		return err
	}

	// As single key range usually represents a single shard, so we batch deletions here
	// without introducing distributed transactions.
	var (
		docIds []uint64
		docID  uint64
		ts     uint64
	)
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			err = res.ScanWithDefaults(&docID, &ts)
			if err != nil {
				return err
			}

			if ts <= timestamp {
				docIds = append(docIds, docID)
			}
			if len(docIds) >= deleteBatchSize {
				if err := deleteExpiredDocuments(ctx, c, prefix, docIds, timestamp); err != nil {
					return err
				}
				docIds = []uint64{}
			}
		}
		if len(docIds) > 0 {
			if err := deleteExpiredDocuments(ctx, c, prefix, docIds, timestamp); err != nil {
				return err
			}
			docIds = []uint64{}
		}
	}

	return nil
}

func deleteExpired(ctx context.Context, c table.Client, prefix string, timestamp uint64) error {
	fmt.Printf("> DeleteExpired: timestamp: %v:\n", timestamp)

	var res options.Description
	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			res, err = s.DescribeTable(ctx, path.Join(prefix, "documents"), options.WithShardKeyBounds())
			return err
		},
	)

	if err != nil {
		return err
	}
	for _, kr := range res.KeyRanges {
		// DeleteExpiredRange can be run in parallel for different ranges.
		// Keep in mind that deletion RPS should be somehow limited in this case to avoid
		// spikes of cluster load due to TTL.
		err = deleteExpiredRange(ctx, c, prefix, timestamp, kr)
		if err != nil {
			return err
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
			_, res, err = s.Execute(ctx, readTx, query, table.NewQueryParameters(
				table.ValueParam("$url", types.UTF8Value(url))),
				options.WithQueryCachePolicy(
					options.WithQueryCachePolicyKeepInCache(),
				),
			)
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

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $url AS Utf8;
        DECLARE $html AS Utf8;
        DECLARE $timestamp AS Uint64;

        $doc_id = Digest::CityHash($url);

        REPLACE INTO documents
            (doc_id, url, html, ts)
        VALUES
            ($doc_id, $url, $html, $timestamp);`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$url", types.UTF8Value(url)),
				table.ValueParam("$html", types.UTF8Value(html)),
				table.ValueParam("$timestamp", types.Uint64Value(timestamp))),
				options.WithQueryCachePolicy(
					options.WithQueryCachePolicyKeepInCache()))
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
						options.WithPartitioningPolicyUniformPartitions(uint64(docTablePartitionCount)))),
			)
		},
	)
	if err != nil {
		return err
	}

	return nil
}
