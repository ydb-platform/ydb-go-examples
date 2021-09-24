package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"text/template"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/resultset"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-examples/pkg/cli"
)

type templateConfig struct {
	TablePathPrefix string
}

var fill = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

DECLARE $seriesData AS List<Struct<
	series_id: Uint64,
	title: Utf8,
	series_info: Utf8,
	release_date: Date,
	comment: Optional<Utf8>>>;

DECLARE $seasonsData AS List<Struct<
	series_id: Uint64,
	season_id: Uint64,
	title: Utf8,
	first_aired: Date,
	last_aired: Date>>;

DECLARE $episodesData AS List<Struct<
	series_id: Uint64,
	season_id: Uint64,
	episode_id: Uint64,
	title: Utf8,
	air_date: Date>>;

REPLACE INTO series
SELECT
	series_id,
	title,
	series_info,
	CAST(release_date AS Uint64) AS release_date,
	comment
FROM AS_TABLE($seriesData);

REPLACE INTO seasons
SELECT
	series_id,
	season_id,
	title,
	CAST(first_aired AS Uint64) AS first_aired,
	CAST(last_aired AS Uint64) AS last_aired
FROM AS_TABLE($seasonsData);

REPLACE INTO episodes
SELECT
	series_id,
	season_id,
	episode_id,
	title,
	CAST(air_date AS Uint64) AS air_date
FROM AS_TABLE($episodesData);
`))

type Command struct {
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := ydb.New(
		connectCtx,
		params.ConnectParams,
		environ.WithEnvironCredentials(ctx),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		ydb.WithSessionPoolKeepAliveMinSize(-1),
		ydb.WithDiscoveryInterval(5*time.Second),
	)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = db.Close() }()

	err = db.Scheme().CleanupDatabase(ctx, params.Prefix(), "series", "episodes", "seasons")
	if err != nil {
		return err
	}

	err = db.Scheme().EnsurePathExists(ctx, params.Prefix())
	if err != nil {
		return err
	}

	err = describeTableOptions(ctx, db.Table())
	if err != nil {
		return fmt.Errorf("describe table options error: %w", err)
	}

	err = createTables(ctx, db.Table(), params.Prefix())
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}

	err = describeTable(ctx, db.Table(), path.Join(
		params.Prefix(), "series",
	))
	if err != nil {
		return fmt.Errorf("describe table error: %w", err)
	}

	err = fillTablesWithData(ctx, db.Table(), params.Prefix())
	if err != nil {
		return fmt.Errorf("fill tables with data error: %w", err)
	}

	err = selectSimple(ctx, db.Table(), params.Prefix())
	if err != nil {
		return fmt.Errorf("select simple error: %w", err)
	}

	err = scanQuerySelect(ctx, db.Table(), params.Prefix())
	if err != nil {
		return fmt.Errorf("scan query select error: %w", err)
	}

	err = readTable(ctx, db.Table(), path.Join(
		params.Prefix(), "series",
	))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	return nil
}

func readTable(ctx context.Context, c table.Client, path string) (err error) {
	var res resultset.Result
	err, issues := c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path,
				options.ReadOrdered(),
				options.ReadColumn("series_id"),
				options.ReadColumn("title"),
				options.ReadColumn("release_date"),
			)
			return
		},
	)
	if err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("\n> readTable issues:\n")
		for _, e := range issues {
			log.Printf("\t> %v\n", e)
		}
		return err
	}
	log.Printf("\n> read_table:")
	var (
		id    *uint64
		title *string
		date  *uint64
	)
	for res.NextResultSet(ctx, "series_id", "title", "release_date") {
		for res.NextRow() {
			_ = res.Scan(&id, &title, &date)

			log.Printf("#  %d %s %d", *id, *title, *date)
		}
	}
	if err := res.Err(); err != nil {
		return err
	}
	stats := res.Stats()
	for i := 0; ; i++ {
		phase, ok := stats.NextPhase()
		if !ok {
			break
		}
		log.Printf(
			"# phase #%d: took %s",
			i, phase.Duration,
		)
		for {
			tbl, ok := phase.NextTableAccess()
			if !ok {
				break
			}
			log.Printf(
				"#  accessed %s: read=(%drows, %dbytes)",
				tbl.Name, tbl.Reads.Rows, tbl.Reads.Bytes,
			)
		}
	}
	return nil
}

func describeTableOptions(ctx context.Context, c table.Client) (err error) {
	var desc options.TableOptionsDescription
	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) (err error) {
			desc, err = s.DescribeTableOptions(ctx)
			return
		},
	)
	if err != nil {
		return err
	}
	log.Println("\n> describe_table_options:")

	for i, p := range desc.TableProfilePresets {
		log.Printf("TableProfilePresets: %d/%d: %+v", i+1, len(desc.TableProfilePresets), p)
	}
	for i, p := range desc.StoragePolicyPresets {
		log.Printf("StoragePolicyPresets: %d/%d: %+v", i+1, len(desc.StoragePolicyPresets), p)
	}
	for i, p := range desc.CompactionPolicyPresets {
		log.Printf("CompactionPolicyPresets: %d/%d: %+v", i+1, len(desc.CompactionPolicyPresets), p)
	}
	for i, p := range desc.PartitioningPolicyPresets {
		log.Printf("PartitioningPolicyPresets: %d/%d: %+v", i+1, len(desc.PartitioningPolicyPresets), p)
	}
	for i, p := range desc.ExecutionPolicyPresets {
		log.Printf("ExecutionPolicyPresets: %d/%d: %+v", i+1, len(desc.ExecutionPolicyPresets), p)
	}
	for i, p := range desc.ReplicationPolicyPresets {
		log.Printf("ReplicationPolicyPresets: %d/%d: %+v", i+1, len(desc.ReplicationPolicyPresets), p)
	}
	for i, p := range desc.CachingPolicyPresets {
		log.Printf("CachingPolicyPresets: %d/%d: %+v", i+1, len(desc.CachingPolicyPresets), p)
	}

	return nil
}

func selectSimple(ctx context.Context, c table.Client, prefix string) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			DECLARE $seriesID AS Uint64;
			$format = DateTime::Format("%Y-%m-%d");
			SELECT
				series_id,
				title,
				$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
			FROM
				series
			WHERE
				series_id = $seriesID;
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	var res resultset.Result
	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$seriesID", types.Uint64Value(1)),
				),
				options.WithQueryCachePolicy(
					options.WithQueryCachePolicyKeepInCache(),
				),
				options.WithCollectStatsModeBasic(),
			)
			return
		},
	)
	if err != nil {
		return err
	}

	var (
		id    *uint64
		title *string
		date  *[]byte
	)
	// TODO(kamardin): truncated flag.
	for res.NextResultSet(ctx, "series_id", "title", "release_date") {
		for res.NextRow() {

			_ = res.Scan(&id, &title, &date)

			log.Printf(
				"\n> select_simple_transaction: %d %s %s",
				*id, *title, *date,
			)
		}
	}
	if err = res.Err(); err != nil {
		return err
	}
	return nil
}

func scanQuerySelect(ctx context.Context, c table.Client, prefix string) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $series AS List<UInt64>;

			SELECT series_id, season_id, title, CAST(CAST(first_aired AS Date) AS String) AS first_aired
			FROM seasons
			WHERE series_id IN $series
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)

	var res resultset.Result
	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) (err error) {
			res, err = s.StreamExecuteScanQuery(ctx, query,
				table.NewQueryParameters(
					table.ValueParam("$series",
						types.ListValue(
							types.Uint64Value(1),
							types.Uint64Value(10),
						),
					),
				),
			)
			return
		},
	)
	if err != nil {
		return err
	}
	var (
		seriesId uint64
		seasonId uint64
		title    string
		date     string
	)
	log.Print("\n> scan_query_select:")
	for res.NextResultSet(ctx, "series_id", "season_id", "title", "first_aired") {
		if err = res.Err(); err != nil {
			return err
		}

		for res.NextRow() {
			err = res.ScanWithDefaults(&seriesId, &seasonId, &title, &date)
			if err != nil {
				return err
			}
			log.Printf("#  Season, SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s", seriesId, seasonId, title, date)
		}
	}
	if err = res.Err(); err != nil {
		return err
	}
	return nil
}

func fillTablesWithData(ctx context.Context, c table.Client, prefix string) (err error) {
	// Prepare write transaction.
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, render(fill, templateConfig{
				TablePathPrefix: prefix,
			}))
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$seriesData", getSeriesData()),
				table.ValueParam("$seasonsData", getSeasonsData()),
				table.ValueParam("$episodesData", getEpisodesData()),
			))
			return err
		},
	)
	return err
}

func createTables(ctx context.Context, c table.Client, prefix string) (err error) {
	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "series"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeUint64)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
	)
	if err != nil {
		return err
	}

	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "seasons"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("season_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("first_aired", types.Optional(types.TypeUint64)),
				options.WithColumn("last_aired", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("series_id", "season_id"),
			)
		},
	)
	if err != nil {
		return err
	}

	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "episodes"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("season_id", types.Optional(types.TypeUint64)),
				options.WithColumn("episode_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("air_date", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			)
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func describeTable(ctx context.Context, c table.Client, path string) (err error) {
	err, _ = c.Retry(ctx, false,
		func(ctx context.Context, s table.Session) error {
			desc, err := s.DescribeTable(ctx, path)
			if err != nil {
				return err
			}
			log.Printf("\n> describe table: %s", path)
			for _, c := range desc.Columns {
				log.Printf("column, name: %s, %s", c.Type, c.Name)
			}
			return nil
		},
	)
	return
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
