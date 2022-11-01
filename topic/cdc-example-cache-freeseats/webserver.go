package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var errNotEnthoughtFreeSeats = errors.New("not enough free seats")

type server struct {
	cache     *Cache
	db        ydb.Connection
	dbCounter int64
	id        int
}

func newServer(id int, db ydb.Connection, cacheTimeout time.Duration) *server {
	res := &server{
		cache: NewCache(cacheTimeout),
		db:    db,
		id:    id,
	}

	return res
}

func (s *server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		if request.URL.Path == "/" {
			s.IndexPageHandler(writer, request)
		} else {
			s.GetHandler(writer, request)
		}
	case http.MethodPost:
		s.PostHandler(writer, request)
	default:
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// GetHandler return free seats
func (s *server) GetHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	id := strings.TrimPrefix(request.URL.Path, "/")

	start := time.Now()
	freeSeats, err := s.getFreeSeats(ctx, id)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	duration := time.Since(start)
	s.writeAnswer(writer, freeSeats, duration)
}

// PostHandler sell seats from bus id
func (s *server) PostHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	id := strings.TrimPrefix(request.URL.Path, "/")

	start := time.Now()
	freeSeats, err := s.sellTicket(ctx, id)
	if err != nil {
		if errors.Is(err, errNotEnthoughtFreeSeats) {
			http.Error(writer, "Not enough free seats", http.StatusPreconditionFailed)
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	// s.cache.Delete(id) // used without cdc, for single-instance application
	duration := time.Since(start)
	s.writeAnswer(writer, freeSeats, duration)
}

func (s *server) writeAnswer(writer http.ResponseWriter, freeSeats int64, duration time.Duration) {
	_, _ = fmt.Fprintf(writer, "%v\n\nDuration: %v\n", freeSeats, duration)
}

func (s *server) getFreeSeats(ctx context.Context, id string) (int64, error) {
	if content, ok := s.cache.Get(id); ok {
		return content, nil
	}

	freeSeats, err := s.getContentFromDB(ctx, id)

	if err == nil {
		s.cache.Set(id, freeSeats)
	}

	return freeSeats, err
}

func (s *server) getContentFromDB(ctx context.Context, id string) (int64, error) {
	atomic.AddInt64(&s.dbCounter, 1)
	var freeSeats int64
	err := s.db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		var err error
		freeSeats, err = s.getFreeSeatsTx(ctx, tx, id)
		return err
	})

	return freeSeats, err
}

func (s *server) getFreeSeatsTx(ctx context.Context, tx table.TransactionActor, id string) (int64, error) {
	var freeSeats int64
	res, err := tx.Execute(ctx, `
DECLARE $id AS Utf8;

SELECT freeSeats FROM bus WHERE id=$id;
`, table.NewQueryParameters(table.ValueParam("$id", types.UTF8Value(id))))
	if err != nil {
		return 0, err
	}

	err = res.NextResultSetErr(ctx, "freeSeats")
	if err != nil {
		return 0, err
	}

	if !res.NextRow() {
		freeSeats = 0
		return 0, errors.New("not found")
	}

	err = res.ScanWithDefaults(&freeSeats)
	if err != nil {
		return 0, err
	}

	return freeSeats, nil
}

func (s *server) sellTicket(ctx context.Context, id string) (int64, error) {
	var freeSeats int64
	err := s.db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		var err error
		freeSeats, err = s.getFreeSeatsTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if freeSeats < 0 {
			return fmt.Errorf("failed to sell ticket: %w", errNotEnthoughtFreeSeats)
		}

		_, err = tx.Execute(ctx, `
DECLARE $id AS Utf8;

UPDATE bus SET freeSeats = freeSeats - 1 WHERE id=$id;
`, table.NewQueryParameters(table.ValueParam("$id", types.UTF8Value(id))))
		return err
	})
	if err == nil {
		freeSeats--
	}
	return freeSeats, err
}

func (s *server) IndexPageHandler(writer http.ResponseWriter, request *http.Request) {
	_, _ = io.WriteString(writer, `Use requests:
GET /1 - for get tickets for bus 1
GET /2A - for get tickets for bus 2A
POST /1 - for sell ticket for bus 1
POST /2A - for sell ticket for bus 2A
`)
}
