package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache/v3"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type dbServer struct {
	cache        *bigcache.BigCache
	cacheEnabled bool
	db           ydb.Connection
	dbCounter    int64
	id           int
}

func newServer(id int, db ydb.Connection, cacheTimeout time.Duration, logCacheRemoved bool) *dbServer {
	cacheCfg := bigcache.DefaultConfig(cacheTimeout)

	if logCacheRemoved {
		cacheCfg.OnRemoveWithReason = func(key string, entry []byte, reason bigcache.RemoveReason) {
			log.Printf("cache removed with from server '%v' reason %v for id: %v", id, reason, key)
		}
	}

	cache, err := bigcache.NewBigCache(cacheCfg)
	if err != nil {
		panic(err)
	}

	res := &dbServer{
		cache:        cache,
		cacheEnabled: cacheTimeout > 0,
		db:           db,
		id:           id,
	}

	if *enableCDC {
		go res.cdcLoop()
	}

	return res
}

func (s *dbServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	id := strings.TrimPrefix(request.URL.Path, "/")
	if id == "" {
		id = "index"
	}

	start := time.Now()
	text, err := s.getContent(ctx, id)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	duration := time.Since(start)
	_, _ = fmt.Fprintf(writer, "Duration: %v\n\n%v", duration, text)
}

func (s *dbServer) getContent(ctx context.Context, id string) (string, error) {
	if content, ok := s.getContentFromCache(id); ok {
		return content, nil
	}

	content, err := s.getContentFromDB(ctx, id)

	if err == nil {
		s.storeInCache(id, content)
	}

	return content, err
}

func (s *dbServer) getContentFromDB(ctx context.Context, id string) (string, error) {
	atomic.AddInt64(&s.dbCounter, 1)
	var freeSeats int64
	err := s.db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, `
DECLARE $id AS Utf8;

SELECT freeSeats FROM bus WHERE id=$id;
`, table.NewQueryParameters(table.ValueParam("$id", types.UTF8Value(id))))
		if err != nil {
			return err
		}

		err = res.NextResultSetErr(ctx, "freeSeats")
		if err != nil {
			return err
		}

		if !res.NextRow() {
			freeSeats = 0
			return errors.New("not found")
		}

		err = res.ScanWithDefaults(&freeSeats)
		if err != nil {
			return err
		}

		return nil
	})

	return fmt.Sprint(freeSeats), err
}

func (s *dbServer) getContentFromCache(id string) (content string, ok bool) {
	if !s.cacheEnabled {
		return "", false
	}
	contentS, err := s.cache.Get(id)
	content = string(contentS)
	return content, err == nil
}

func (s *dbServer) storeInCache(id, content string) {
	if !s.cacheEnabled {
		return
	}
	log.Printf("server id: %v store cache for article: %v ('%v')", s.id, id, content)
	_ = s.cache.Set(id, []byte(content))
}
