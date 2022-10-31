package main

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func createTables(ctx context.Context, db ydb.Connection) error {
	_, err := db.Scripting().Execute(ctx, `
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
UPSERT INTO bus (id, freeSeats) VALUES ("1", 40), ("2A", 60);
`, nil)
	if err != nil {
		return fmt.Errorf("failed insert pages: %w", err)
	}
	return nil
}
