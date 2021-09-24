package main

import (
	"context"
	"fmt"

	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/resultset"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func doDelete(
	ctx context.Context,
	c table.Client,
	prefix string,
	args ...string,
) error {
	if len(args) == 0 {
		return fmt.Errorf("id of series arguemnt required")
	}
	s, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return err
	}

	count, err := deleteTransaction(ctx, c, prefix, s)
	if err != nil {
		return err
	}
	fmt.Printf("Deleted %v rows", count)
	return nil
}

func deleteTransaction(ctx context.Context, c table.Client, prefix string, seriesID uint64,
) (count uint64, err error) {
	query := fmt.Sprintf(`
        PRAGMA TablePathPrefix("%v");

        DECLARE $seriesId AS Uint64;

        -- Simulate a DESC index by inverting views using max(uint64)-views
        $maxUint64 = 0xffffffffffffffff;

        $data = (
            SELECT series_id, ($maxUint64 - views) AS rev_views
            FROM `+"`series`"+`
            WHERE series_id = $seriesId
        );

        DELETE FROM series
        ON SELECT series_id FROM $data;

        DELETE FROM series_rev_views
        ON SELECT rev_views, series_id FROM $data;

        SELECT COUNT(*) AS cnt FROM $data;`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	var res resultset.Result
	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, writeTx,
				table.NewQueryParameters(
					table.ValueParam("$seriesId", types.Uint64Value(seriesID)),
				))
			return err
		})
	if err != nil {
		return
	}
	if res.NextResultSet(ctx) && res.NextRow() {
		err = res.Scan(&count)
		if err != nil {
			return
		}
	}
	return
}
