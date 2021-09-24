package main

import "strconv"

var (
	_ = strconv.Itoa
)

type Item struct {
	HostUID uint64 `ydb:"column:host_uid"`
	URLUID  uint64 `ydb:"column:url_uid"`
	URL     string `ydb:"column:url"`
	Page    string
}

type ItemList []Item
