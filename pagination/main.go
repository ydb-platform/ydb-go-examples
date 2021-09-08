package main

import "github.com/ydb-platform/ydb-go-examples/pkg/cli"

func main() {
	cli.Run(new(Command))
}
