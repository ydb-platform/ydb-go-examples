package main

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func createTables(ctx context.Context, db ydb.Connection) error {
	_, err := db.Scripting().Execute(ctx, `
CREATE TABLE articles (id Utf8, text Utf8, PRIMARY KEY(id));

ALTER TABLE 
	articles
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
UPSERT INTO articles (id, text) VALUES ("index", "Hello"), ("1", "Page 1");
`, nil)
	if err != nil {
		return fmt.Errorf("failed insert pages: %w", err)
	}
	return nil
}
