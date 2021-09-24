package main

import (
	"context"
	"log"
	"os"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func doCreate(
	ctx context.Context,
	c table.Client,
	prefix string,
	args ...string,
) error {
	for _, desc := range []struct {
		name string
		opts []options.CreateTableOption
	}{
		{
			name: TableSeries,
			opts: []options.CreateTableOption{
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeDatetime)),
				options.WithColumn("views", types.Optional(types.TypeUint64)),

				options.WithPrimaryKeyColumn("series_id"),
			},
		},
		{
			name: TableSeriesRevViews,
			opts: []options.CreateTableOption{
				options.WithColumn("rev_views", types.Optional(types.TypeUint64)),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),

				options.WithPrimaryKeyColumn("rev_views", "series_id"),
			},
		},
	} {
		err, issues := c.Retry(ctx, false,
			func(ctx context.Context, s table.Session) error {
				return s.CreateTable(ctx, path.Join(prefix, desc.name), desc.opts...)
			},
		)
		if err != nil {
			log.SetOutput(os.Stderr)
			log.Printf("\n> doCreate issues:\n")
			for _, e := range issues {
				log.Printf("\t> %v\n", e)
			}
			return err
		}
	}
	return nil
}
