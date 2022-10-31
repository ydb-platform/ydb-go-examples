package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func createTableAndCDC(ctx context.Context, db ydb.Connection, consumersCount int) {
	err := createTables(ctx, db)
	if err != nil {
		log.Fatalf("failed to create tables: %+v", err)
	}

	err = createCosumers(ctx, db, consumersCount)
	if err != nil {
		log.Fatalf("failed to create consumers: %+v", err)
	}
}

func createTables(ctx context.Context, db ydb.Connection) error {
	_, err := db.Scripting().Execute(ctx, `
DROP TABLE bus;

CREATE TABLE bus (id Utf8, freeSeats Int64, PRIMARY KEY(id));

ALTER TABLE 
	bus
ADD CHANGEFEED
	updates
WITH (
	FORMAT = 'JSON',
	MODE = 'UPDATES'
)
`, nil)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	_, err = db.Scripting().Execute(ctx, `
UPSERT INTO bus (id, freeSeats) VALUES ("", 0), ("1", 40), ("2A", 60);
`, nil)
	if err != nil {
		return fmt.Errorf("failed insert pages: %w", err)
	}
	return nil
}

func createCosumers(ctx context.Context, db ydb.Connection, consumersCount int) error {
	for i := 0; i < consumersCount; i++ {
		err := db.Topic().Alter(ctx, "bus/updates", topicoptions.AlterWithAddConsumers(topictypes.Consumer{
			Name: consumerName(i),
		}))
		if err != nil {
			return err
		}
	}

	return nil
}
