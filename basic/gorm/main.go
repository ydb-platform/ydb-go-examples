package main

import (
	"errors"
	"log"
	"os"
	"time"

	ydb "github.com/ydb-platform/gorm-driver"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func initDB() (*gorm.DB, error) {
	if dsn, has := os.LookupEnv("POSTGRES_CONNECTION_STRING"); has {
		return gorm.Open(postgres.New(postgres.Config{
			DSN:                  dsn,
			PreferSimpleProtocol: true,
		}))
	}
	if dsn, has := os.LookupEnv("SQLITE_CONNECTION_STRING"); has {
		return gorm.Open(sqlite.Open(dsn))
	}
	if dsn, has := os.LookupEnv("YDB_CONNECTION_STRING"); has {
		return gorm.Open(ydb.Open(dsn))
	}
	return nil, errors.New("cannot initialize DB")
}

const dateISO8601 = "2006-01-02"

func date(date string) time.Time {
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}

func main() {
	// connect
	db, err := initDB()
	if err != nil {
		panic(err)
	}

	// prepare scheme and migrations
	if err = db.AutoMigrate(
		&Series{},
		&Season{},
		&Episode{},
	); err != nil {
		panic(err)
	}

	// upsert data
	if err = db.Create(data).Error; err != nil {
		panic(err)
	}

	// get all series
	var series []Series
	if err = db.Preload("Seasons.Episodes").Find(&series).Error; err != nil {
		panic(err)
	}
	log.Println("all known series:")
	for _, s := range series {
		log.Printf(
			"  > [%s]     %s (%s)\n",
			s.ID, s.Title, s.ReleaseDate.Format("2006"),
		)
		for _, ss := range s.Seasons {
			log.Printf(
				"    > [%s]   %s\n",
				ss.ID, ss.Title,
			)
			for _, e := range ss.Episodes {
				log.Printf(
					"      > [%s] [%s] %s\n",
					e.ID, e.AirDate.Format(dateISO8601), e.Title,
				)
			}
		}
	}
}
