package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func doDescribe(
	ctx context.Context,
	c table.Client,
	prefix string,
	args ...string,
) error {
	for _, name := range []string{"series", "users"} {
		var desc options.Description
		err, issues := c.Retry(ctx, false,
			func(ctx context.Context, s table.Session) (err error) {
				desc, err = s.DescribeTable(ctx, path.Join(prefix, name))
				return
			},
		)
		if err != nil {
			log.SetOutput(os.Stderr)
			log.Printf("\n> doDescribe issues:\n")
			for _, e := range issues {
				log.Printf("\t> %v\n", e)
			}
			return err
		}
		p, err := json.MarshalIndent(desc, "", "  ")
		if err != nil {
			return err
		}
		fmt.Printf("Describe %v table: %s \n", name, p)
	}
	return nil
}
