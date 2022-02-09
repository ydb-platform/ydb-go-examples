package main

import (
	"context"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

type row struct {
	id          uint64
	orderID     uint64
	date        time.Time
	description string
}

func readTable(ctx context.Context, c table.Client, path string, opts ...options.ReadTableOption) (err error) {
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			res, err := s.StreamReadTable(ctx, path, opts...)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			r := row{}
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					if res.CurrentResultSet().ColumnCount() == 4 {
						err = res.ScanNamed(
							named.OptionalWithDefault("customer_id", &r.id),
							named.OptionalWithDefault("order_id", &r.orderID),
							named.OptionalWithDefault("order_date", &r.date),
							named.OptionalWithDefault("description", &r.description),
						)
						if err != nil {
							return err
						}
						log.Printf("#  Order, CustomerId: %d, OrderId: %d, Description: %s, Order date: %s", r.id, r.orderID, r.description, r.date.Format("2006-01-02"))
					} else {
						err = res.ScanNamed(
							named.OptionalWithDefault("customer_id", &r.id),
							named.OptionalWithDefault("order_id", &r.orderID),
							named.OptionalWithDefault("order_date", &r.date),
						)
						if err != nil {
							return err
						}
						log.Printf("#  Order, CustomerId: %d, OrderId: %d, Order date: %s", r.id, r.orderID, r.date.Format("2006-01-02"))
					}
				}
			}
			return res.Err()
		},
	)
	return err
}
