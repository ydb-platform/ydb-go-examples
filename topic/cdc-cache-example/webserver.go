package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type dbServer struct {
	cache        *Cache
	cacheEnabled bool
	db           ydb.Connection
	dbCounter    int64
	id           int
}

func newServer(id int, db ydb.Connection, cacheTimeout time.Duration) *dbServer {
	res := &dbServer{
		cache: NewCache(cacheTimeout),
		db:    db,
		id:    id,
	}

	if !*disableCDC {
		go res.cdcLoop()
	}

	return res
}

func (s *dbServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		s.GetHandler(writer, request)
	case http.MethodPost:
		s.PostHandler(writer, request)
	default:
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *dbServer) GetHandler(writer http.ResponseWriter, request *http.Request) {
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

func (s *dbServer) PostHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	id := strings.TrimPrefix(request.URL.Path, "/")

	start := time.Now()
	freeSeats, err := s.sellTicket(ctx, id)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	// s.cache.Delete(id)
	duration := time.Since(start)
	s.writeAnswer(writer, freeSeats, duration)
}

func (s *dbServer) writeAnswer(writer http.ResponseWriter, freeSeats int64, duration time.Duration) {
	_, _ = fmt.Fprintf(writer, "%v\n\nDuration: %v\n", freeSeats, duration)
}

func (s *dbServer) getFreeSeats(ctx context.Context, id string) (int64, error) {
	if content, ok := s.cache.Get(id); ok {
		return content, nil
	}

	freeSeats, err := s.getContentFromDB(ctx, id)

	if err == nil {
		s.cache.Set(id, freeSeats)
	}

	return freeSeats, err
}

func (s *dbServer) getContentFromDB(ctx context.Context, id string) (int64, error) {
	atomic.AddInt64(&s.dbCounter, 1)
	var freeSeats int64
	err := s.db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		var err error
		freeSeats, err = s.getFreeSeatsTx(ctx, tx, id)
		return err
	})

	return freeSeats, err
}

func (s *dbServer) getFreeSeatsTx(ctx context.Context, tx table.TransactionActor, id string) (int64, error) {
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

func (s *dbServer) sellTicket(ctx context.Context, id string) (int64, error) {
	var freeSeats int64
	err := s.db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		var err error
		freeSeats, err = s.getFreeSeatsTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if freeSeats < 0 {
			return errors.New("not enough free seats")
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
