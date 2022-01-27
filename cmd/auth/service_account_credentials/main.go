package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	yc "github.com/ydb-platform/ydb-go-yc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		yc.WithInternalCA(),
		yc.WithServiceAccountKeyFileCredentials(os.Getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close(ctx) }()

	whoAmI, err := db.Discovery().WhoAmI(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println(whoAmI.String())
}
