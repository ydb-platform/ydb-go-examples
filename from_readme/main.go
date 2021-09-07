package main

import (
	"github.com/YandexDatabase/ydb-go-examples/pkg/cli"
)

func main() {
	cli.Run(new(Command))
}
