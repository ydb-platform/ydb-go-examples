package cli

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

var ErrPrintUsage = fmt.Errorf("")

type Parameters struct {
	Args          []string
	ConnectParams ydb.ConnectParams

	link   string
	prefix string
}

func (p *Parameters) Database() string {
	return p.ConnectParams.Database()
}

func (p *Parameters) Prefix() string {
	return path.Join(p.Database(), p.prefix)
}

type Command interface {
	// Run runs logic of program
	Run(context.Context, Parameters) error
	// ExportFlags provide cli flag set extension with custom program flags
	ExportFlags(context.Context, *flag.FlagSet)
}

type CommandFunc func(context.Context, Parameters) error

func (f CommandFunc) Run(ctx context.Context, params Parameters) error {
	return f(ctx, params)
}

func (f CommandFunc) ExportFlags(context.Context, *flag.FlagSet) {}

// Run makes common initialization and runs custom command
func Run(cmd Command) {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s command [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}

	var params Parameters
	flagSet.StringVar(&params.link,
		"ydb", "",
		"YDB connection string",
	)
	flagSet.StringVar(&params.prefix,
		"prefix", "",
		"tables prefix",
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.ExportFlags(ctx, flagSet)

	_ = flagSet.Parse(os.Args[1:])

	params.Args = flagSet.Args()

	params.ConnectParams = ydb.MustConnectionString(params.link)

	quit := make(chan error)
	go processSignals(map[os.Signal]func(){
		syscall.SIGINT: func() {
			if ctx.Err() != nil {
				quit <- fmt.Errorf("forced quit")
			}
			cancel()
		},
	})

	log.SetFlags(0)

	done := make(chan error)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				buf := make([]byte, 64<<10)
				buf = buf[:runtime.Stack(buf, false)]
				done <- fmt.Errorf("panic recovered: %v\n%s", e, buf)
			}
		}()
		done <- cmd.Run(ctx, params)
	}()

	var err error
	select {
	case err = <-done:
	case err = <-quit:
	}
	if err == ErrPrintUsage {
		flagSet.Usage()
		os.Exit(1)
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func processSignals(m map[os.Signal]func()) {
	ch := make(chan os.Signal, len(m))
	for sig := range m {
		signal.Notify(ch, sig)
	}
	for sig := range ch {
		log.Printf("signal received: %s", sig)
		m[sig]()
	}
}
