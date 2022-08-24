package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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
	db, err := sql.Open("ydb", dsn)
	if err != nil {
		log.Fatalf("connect error: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cc, err := ydb.Unwrap(db)

	prefix = path.Join(cc.Name(), prefix)

	err = sugar.RemoveRecursive(ctx, cc, prefix)
	if err != nil {
		log.Fatalf("remove recursive failed: %v", err)
	}

	err = prepareSchema(ctx, db, prefix)
	if err != nil {
		log.Fatalf("create tables error: %v", err)
	}

	err = fillTablesWithData(ctx, db, prefix)
	if err != nil {
		log.Fatalf("fill tables with data error: %v", err)
	}

	err = selectDefault(ctx, db, prefix)
	if err != nil {
		log.Fatal(err)
	}

	err = selectScan(ctx, db, prefix)
	if err != nil {
		log.Fatal(err)
	}
}

func selectDefault(ctx context.Context, db *sql.DB, prefix string) (err error) {
	query := `
		PRAGMA TablePathPrefix("` + prefix + `");

		SELECT series_id, title, release_date FROM series;
	`
	// explain of query
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
		row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ExplainQueryMode), query)
		var (
			ast  string
			plan string
		)
		if err = row.Scan(&ast, &plan); err != nil {
			return err
		}
		//log.Printf("AST = %s\n\nPlan = %s", ast, plan)
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("explain query `%s` failed: %w", strings.ReplaceAll(query, "\n", `\n`), err)
	}
	err = retry.Do(ydb.WithTxControl(ctx, table.OnlineReadOnlyTxControl()), db, func(ctx context.Context, cc *sql.Conn) (err error) {
		rows, err := cc.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		var (
			id          *string
			title       *string
			releaseDate *time.Time
		)
		log.Println("> select of all known series:")
		for rows.Next() {
			if err = rows.Scan(&id, &title, &releaseDate); err != nil {
				return err
			}
			log.Printf(
				"> [%s] %s (%s)",
				*id, *title, releaseDate.Format("2006-01-02"),
			)
		}
		return rows.Err()
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("execute data query `%s` failed: %w", strings.ReplaceAll(query, "\n", `\n`), err)
	}
	return nil
}

func selectScan(ctx context.Context, db *sql.DB, prefix string) (err error) {
	seriesIDsQuery := `
		PRAGMA TablePathPrefix("` + prefix + `");

		DECLARE $seriesTitle AS Utf8;

		SELECT 			series_id 		
		FROM 			series
		WHERE 			title LIKE $seriesTitle;
	`
	seasonIDsQuery := `
		PRAGMA TablePathPrefix("` + prefix + `");

		DECLARE $seasonTitle AS Utf8;

		SELECT 			season_id 		
		FROM 			seasons
		WHERE 			title LIKE $seasonTitle
	`
	query := `
		PRAGMA TablePathPrefix("` + prefix + `");
		PRAGMA AnsiInForEmptyOrNullableItemsCollections;

		DECLARE $seasonIDs AS List<Bytes>;
		DECLARE $seriesIDs AS List<Bytes>;
		DECLARE $from AS Date;
		DECLARE $to AS Date;

		SELECT 
			episode_id, title, air_date FROM episodes
		WHERE 	
			series_id IN $seriesIDs 
			AND season_id IN $seasonIDs 
			AND air_date BETWEEN $from AND $to;
	`
	// scan query
	err = retry.Do(ydb.WithTxControl(ctx, table.StaleReadOnlyTxControl()), db, func(ctx context.Context, cc *sql.Conn) (err error) {
		var (
			id        string
			seriesIDs []types.Value
			seasonIDs []types.Value
		)
		// getting series ID's
		row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode), seriesIDsQuery,
			sql.Named("seriesTitle", "%IT Crowd%"),
		)
		if err = row.Scan(&id); err != nil {
			return err
		}
		seriesIDs = append(seriesIDs, types.BytesValueFromString(id))
		if err = row.Err(); err != nil {
			return err
		}

		// getting season ID's
		rows, err := cc.QueryContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode), seasonIDsQuery,
			sql.Named("seasonTitle", "%Season 1%"),
		)
		if err != nil {
			return err
		}
		for rows.Next() {
			if err = rows.Scan(&id); err != nil {
				return err
			}
			seasonIDs = append(seasonIDs, types.BytesValueFromString(id))
		}
		if err = rows.Err(); err != nil {
			return err
		}
		_ = rows.Close()

		// getting final query result
		rows, err = cc.QueryContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode), query,
			sql.Named("seriesIDs", types.ListValue(seriesIDs...)),
			sql.Named("seasonIDs", types.ListValue(seasonIDs...)),
			sql.Named("from", date("2006-01-01")),
			sql.Named("to", date("2006-12-31")),
		)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		var (
			episodeID  string
			title      string
			firstAired time.Time
		)
		log.Println("> scan select of episodes of `Season 1` of `IT Crowd` between 2006-01-01 and 2006-12-31:")
		for rows.Next() {
			if err = rows.Scan(&episodeID, &title, &firstAired); err != nil {
				return err
			}
			log.Printf(
				"> [%s] %s (%s)",
				episodeID, title, firstAired.Format("2006-01-02"),
			)
		}
		return rows.Err()
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("scan query `%s` failed: %w", strings.ReplaceAll(query, "\n", `\n`), err)
	}
	return nil
}

func fillTablesWithData(ctx context.Context, db *sql.DB, prefix string) (err error) {
	query := `
		PRAGMA TablePathPrefix("` + prefix + `");

		DECLARE $episodesData AS List<Struct<series_id:String,season_id:String,episode_id:String,title:Utf8,air_date:Date>>;
		DECLARE $seasonsData AS List<Struct<series_id:String,season_id:String,title:Utf8,first_aired:Date,last_aired:Date>>;
		DECLARE $seriesData AS List<Struct<series_id:String,release_date:Date,title:Utf8,series_info:Utf8,comment:Optional<Utf8>>>;

		REPLACE INTO series
		SELECT
			series_id,
			title,
			series_info,
			release_date,
			comment
		FROM AS_TABLE($seriesData);
				
		REPLACE INTO seasons
		SELECT
			series_id,
			season_id,
			title,
			first_aired,
			last_aired
		FROM AS_TABLE($seasonsData);
				
		REPLACE INTO episodes
		SELECT
			series_id,
			season_id,
			episode_id,
			title,
			air_date
		FROM AS_TABLE($episodesData);
	`
	series, seasonsData, episodesData := getData()
	return retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, query,
			sql.Named("seriesData", types.ListValue(series...)),
			sql.Named("seasonsData", types.ListValue(seasonsData...)),
			sql.Named("episodesData", types.ListValue(episodesData...)),
		); err != nil {
			return err
		}
		return nil
	}, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
}

func prepareSchema(ctx context.Context, db *sql.DB, prefix string) (err error) {
	if err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err := cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "series")),
		)
		if err != nil {
			fmt.Fprintf(os.Stdout, "warn: drop series table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`CREATE TABLE `+"`"+path.Join(prefix, "series")+"`"+` (
				series_id Bytes,
				title Utf8,
				series_info Utf8,
				release_date Date,
				comment Utf8,
				INDEX index_series_title GLOBAL ASYNC ON ( title ),
				PRIMARY KEY (
					series_id
				)
			) WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			);`,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create series table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true))); err != nil {
		return err
	}
	if err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "seasons")),
		)
		if err != nil {
			fmt.Fprintf(os.Stdout, "warn: drop seasons table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`CREATE TABLE `+"`"+path.Join(prefix, "seasons")+"`"+` (
				series_id Bytes,
				season_id Bytes,
				title Utf8,
				first_aired Date,
				last_aired Date,
				INDEX index_seasons_title GLOBAL ASYNC ON ( title ),
				INDEX index_seasons_first_aired GLOBAL ASYNC ON ( first_aired ),
				PRIMARY KEY (
					series_id,
					season_id
				)
			) WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			);`,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create seasons table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true))); err != nil {
		return err
	}
	if err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "episodes")),
		)
		if err != nil {
			fmt.Fprintf(os.Stdout, "warn: drop episodes table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`CREATE TABLE `+"`"+path.Join(prefix, "episodes")+"`"+` (
				series_id Bytes,
				season_id Bytes,
				episode_id Bytes,
				title Utf8,
				air_date Date,
				views Uint64,
				INDEX index_episodes_air_date GLOBAL ASYNC ON ( air_date ),
				PRIMARY KEY (
					series_id,
					season_id,
					episode_id
				)
			) WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			);`,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create episodes table failed: %v", err)
			return err
		}

		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true))); err != nil {
		return err
	}
	return nil
}

func seriesData(id string, released time.Time, title, info, comment string) types.Value {
	var commentv types.Value
	if comment == "" {
		commentv = types.NullValue(types.TypeUTF8)
	} else {
		commentv = types.OptionalValue(types.UTF8Value(comment))
	}
	return types.StructValue(
		types.StructFieldValue("series_id", types.BytesValueFromString(id)),
		types.StructFieldValue("release_date", types.DateValueFromTime(released)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("series_info", types.UTF8Value(info)),
		types.StructFieldValue("comment", commentv),
	)
}

func seasonData(seriesID, seasonID string, title string, first, last time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.BytesValueFromString(seriesID)),
		types.StructFieldValue("season_id", types.BytesValueFromString(seasonID)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("first_aired", types.DateValueFromTime(first)),
		types.StructFieldValue("last_aired", types.DateValueFromTime(last)),
	)
}

func episodeData(seriesID, seasonID, episodeID string, title string, date time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.BytesValueFromString(seriesID)),
		types.StructFieldValue("season_id", types.BytesValueFromString(seasonID)),
		types.StructFieldValue("episode_id", types.BytesValueFromString(episodeID)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("air_date", types.DateValueFromTime(date)),
	)
}

func getData() (series []types.Value, seasons []types.Value, episodes []types.Value) {
	for seriesID, fill := range map[string]func(seriesID string) (seriesData types.Value, seasons []types.Value, episodes []types.Value){
		uuid.New().String(): getDataForITCrowd,
		uuid.New().String(): getDataForSiliconValley,
	} {
		seriesData, seasonsData, episodesData := fill(seriesID)
		series = append(series, seriesData)
		seasons = append(seasons, seasonsData...)
		episodes = append(episodes, episodesData...)
	}
	return
}

func getDataForITCrowd(seriesID string) (series types.Value, seasons []types.Value, episodes []types.Value) {
	series = seriesData(
		seriesID, date("2006-02-03"), "IT Crowd", ""+
			"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
			"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
		"", // NULL comment.
	)
	for _, season := range []struct {
		title    string
		first    time.Time
		last     time.Time
		episodes map[string]time.Time
	}{
		{"Season 1", date("2006-02-03"), date("2006-03-03"), map[string]time.Time{
			"Yesterday's Jam":             date("2006-02-03"),
			"Calamity Jen":                date("2006-02-03"),
			"Fifty-Fifty":                 date("2006-02-10"),
			"The Red Door":                date("2006-02-17"),
			"The Haunting of Bill Crouse": date("2006-02-24"),
			"Aunt Irma Visits":            date("2006-03-03"),
		}},
		{"Season 2", date("2007-08-24"), date("2007-09-28"), map[string]time.Time{
			"The Work Outing":            date("2006-08-24"),
			"Return of the Golden Child": date("2007-08-31"),
			"Moss and the German":        date("2007-09-07"),
			"The Dinner Party":           date("2007-09-14"),
			"Smoke and Mirrors":          date("2007-09-21"),
			"Men Without Women":          date("2007-09-28"),
		}},
		{"Season 3", date("2008-11-21"), date("2008-12-26"), map[string]time.Time{
			"From Hell":       date("2008-11-21"),
			"Are We Not Men?": date("2008-11-28"),
			"Tramps Like Us":  date("2008-12-05"),
			"The Speech":      date("2008-12-12"),
			"Friendface":      date("2008-12-19"),
			"Calendar Geeks":  date("2008-12-26"),
		}},
		{"Season 4", date("2010-06-25"), date("2010-07-30"), map[string]time.Time{
			"Jen The Fredo":         date("2010-06-25"),
			"The Final Countdown":   date("2010-07-02"),
			"Something Happened":    date("2010-07-09"),
			"Italian For Beginners": date("2010-07-16"),
			"Bad Boys":              date("2010-07-23"),
			"Reynholm vs Reynholm":  date("2010-07-30"),
		}},
	} {
		seasonID := uuid.New().String()
		seasons = append(seasons, seasonData(seriesID, seasonID, season.title, season.first, season.last))
		for title, date := range season.episodes {
			episodes = append(episodes, episodeData(seriesID, seasonID, uuid.New().String(), title, date))
		}
	}
	return
}

func getDataForSiliconValley(seriesID string) (series types.Value, seasons []types.Value, episodes []types.Value) {
	series = seriesData(
		seriesID, date("2014-04-06"), "Silicon Valley", ""+
			"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
			"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
		"Some comment here",
	)
	for _, season := range []struct {
		title    string
		first    time.Time
		last     time.Time
		episodes map[string]time.Time
	}{
		{"Season 1", date("2006-02-03"), date("2006-03-03"), map[string]time.Time{
			"Minimum Viable Product":        date("2014-04-06"),
			"The Cap Table":                 date("2014-04-13"),
			"Articles of Incorporation":     date("2014-04-20"),
			"Fiduciary Duties":              date("2014-04-27"),
			"Signaling Risk":                date("2014-05-04"),
			"Third Party Insourcing":        date("2014-05-11"),
			"Proof of Concept":              date("2014-05-18"),
			"Optimal Tip-to-Tip Efficiency": date("2014-06-01"),
		}},
		{"Season 2", date("2007-08-24"), date("2007-09-28"), map[string]time.Time{
			"Sand Hill Shuffle":      date("2015-04-12"),
			"Runaway Devaluation":    date("2015-04-19"),
			"Bad Money":              date("2015-04-26"),
			"The Lady":               date("2015-05-03"),
			"Server Space":           date("2015-05-10"),
			"Homicide":               date("2015-05-17"),
			"Adult Content":          date("2015-05-24"),
			"White Hat/Black Hat":    date("2015-05-31"),
			"Binding Arbitration":    date("2015-06-07"),
			"Two Days of the Condor": date("2015-06-14"),
		}},
		{"Season 3", date("2008-11-21"), date("2008-12-26"), map[string]time.Time{
			"Founder Friendly":               date("2016-04-24"),
			"Two in the Box":                 date("2016-05-01"),
			"Meinertzhagen's Haversack":      date("2016-05-08"),
			"Maleant Data Systems Solutions": date("2016-05-15"),
			"The Empty Chair":                date("2016-05-22"),
			"Bachmanity Insanity":            date("2016-05-29"),
			"To Build a Better Beta":         date("2016-06-05"),
			"Bachman's Earnings Over-Ride":   date("2016-06-12"),
			"Daily Active Users":             date("2016-06-19"),
			"The Uptick":                     date("2016-06-26"),
		}},
		{"Season 4", date("2010-06-25"), date("2010-07-30"), map[string]time.Time{
			"Success Failure":       date("2017-04-23"),
			"Terms of Service":      date("2017-04-30"),
			"Intellectual Property": date("2017-05-07"),
			"Teambuilding Exercise": date("2017-05-14"),
			"The Blood Boy":         date("2017-05-21"),
			"Customer Service":      date("2017-05-28"),
			"The Patent Troll":      date("2017-06-04"),
			"The Keenan Vortex":     date("2017-06-11"),
			"Hooli-Con":             date("2017-06-18"),
			"Server Error":          date("2017-06-25"),
		}},
		{"Season 5", date("2018-03-25"), date("2018-05-13"), map[string]time.Time{
			"Grow Fast or Die Slow":             date("2018-03-25"),
			"Reorientation":                     date("2018-04-01"),
			"Chief Operating Officer":           date("2018-04-08"),
			"Tech Evangelist":                   date("2018-04-15"),
			"Facial Recognition":                date("2018-04-22"),
			"Artificial Emotional Intelligence": date("2018-04-29"),
			"Initial Coin Offering":             date("2018-05-06"),
			"Fifty-One Percent":                 date("2018-05-13"),
		}},
	} {
		seasonID := uuid.New().String()
		seasons = append(seasons, seasonData(seriesID, seasonID, season.title, season.first, season.last))
		for title, date := range season.episodes {
			episodes = append(episodes, episodeData(seriesID, seasonID, uuid.New().String(), title, date))
		}
	}
	return
}

const dateISO8601 = "2006-01-02"

func date(date string) time.Time {
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}
