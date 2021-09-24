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
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var ErrPrintUsage = fmt.Errorf("")

type Parameters struct {
	Args           []string
	ConnectParams  ydb.ConnectParams
	ConnectTimeout time.Duration

	link                  string
	prefix                string
	driverTrace           bool
	tableClientTrace      bool
	tableSessionPoolTrace bool
}

func (p *Parameters) Database() string {
	return p.ConnectParams.Database()
}

func (p *Parameters) Prefix() string {
	return path.Join(p.Database(), p.prefix)
}

type Command interface {
	Run(context.Context, Parameters) error
	ExportFlags(context.Context, *flag.FlagSet)
}

type CommandFunc func(context.Context, Parameters) error

func (f CommandFunc) Run(ctx context.Context, params Parameters) error {
	return f(ctx, params)
}

func (f CommandFunc) ExportFlags(context.Context, *flag.FlagSet) {}

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
	flagSet.DurationVar(&params.ConnectTimeout,
		"connect-timeout", time.Second,
		"connect timeout",
	)
	flagSet.BoolVar(&params.driverTrace,
		"driver-trace", false,
		"trace all driver events",
	)
	flagSet.BoolVar(&params.tableClientTrace,
		"table-client-trace", false,
		"trace all table client events",
	)
	flagSet.BoolVar(&params.tableSessionPoolTrace,
		"table-session-pool-trace", false,
		"trace all table session pool events",
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.ExportFlags(ctx, flagSet)

	_ = flagSet.Parse(os.Args[1:])

	params.Args = flagSet.Args()

	params.ConnectParams = ydb.MustConnectionString(params.link)

	if params.driverTrace {
		var t trace.Driver
		trace.Stub(&t, func(name string, args ...interface{}) {
			log.Printf(
				"[driver] %s: %+v",
				name, trace.ClearContext(args),
			)
		})
		ctx = trace.WithDriver(ctx, t)
	}

	if params.tableClientTrace {
		var t trace.Retry
		trace.Stub(&t, func(name string, args ...interface{}) {
			log.Printf(
				"[retry] %s: %+v",
				name, trace.ClearContext(args),
			)
		})
		ctx = trace.WithRetry(ctx, t)
	}

	if params.tableSessionPoolTrace {
		var t trace.Table
		trace.Stub(&t, func(name string, args ...interface{}) {
			log.Printf(
				"[client trace] %s: %+v",
				name, trace.ClearContext(args),
			)
		})
		ctx = trace.WithTable(ctx, t)
	}

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
