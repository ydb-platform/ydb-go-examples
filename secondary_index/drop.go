package main

import (
	"context"
	"log"
	"os"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func doDrop(
	ctx context.Context,
	c table.Client,
	prefix string,
	args ...string,
) error {
	for _, name := range []string{TableSeries, TableSeriesRevViews} {
		err, issues := c.Retry(ctx, false,
			func(ctx context.Context, s table.Session) error {
				return s.DropTable(ctx, path.Join(prefix, name))
			},
		)
		if err != nil {
			log.SetOutput(os.Stderr)
			log.Printf("\n> doDrop issues:\n")
			for _, e := range issues {
				log.Printf("\t> %v\n", e)
			}
			return err
		}
	}
	return nil
}
