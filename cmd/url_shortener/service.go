package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net/http"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	invalidHashError = "'%s' is not a valid short path."
	hashNotFound     = "'%s' path is not found"
	invalidURLError  = "'%s' is not a valid URL."
)

var (
	indexPageContent = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>URL shortener</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
    <script src="http://code.jquery.com/jquery-3.5.0.min.js" integrity="sha256-xNzN2a4ltkB44Mc/Jz3pT4iU1cmeR0FkXs4pru/JxaQ=" crossorigin="anonymous"></script>
    <style>
        body {
            margin-top: 30px;
        }
        .shorten {
            color: blue;
        }
    </style>
</head>
<body>

<div class="container">
    <div class="row-12">
        <form id="source-url-form">
            <div class="form-row">
                <div class="col-12">
                    <input id="source" type="text" class="form-control" placeholder="https://">
                </div>
            </div>
        </form>
    </div>
    <div class="row-12">
        <a id="shorten" class="shorten" href=""/>
    </div>
</div>

<script>
    $(function() {
        let $form = $("#source-url-form");
        let $source = $("#source");
        let $shorten = $("#shorten");

        $form.submit(function(e) {
            e.preventDefault();

            $.ajax({
                url: 'url?url=' + $source.val(),
                contentType: "application/text; charset=utf-8",
                traditional: true,
                success: function(data) {
                    $shorten.html(data);
					$shorten.attr('href', data);
                },
                error: function(data) {
                    $shorten.html('invalid url')
					$shorten.attr('href', '');
                }
            });
        });
    });
</script>

</body>
</html>`

	short = regexp.MustCompile(`[a-zA-Z0-9]`)
	long  = regexp.MustCompile(`https?://(?:[-\w.]|%[\da-fA-F]{2})+`)
)

func hash(s string) (string, error) {
	hasher := fnv.New32a()
	_, err := hasher.Write([]byte(s))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func isShortCorrect(link string) bool {
	return short.FindStringIndex(link) != nil
}

func isLongCorrect(link string) bool {
	return long.FindStringIndex(link) != nil
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

type templateConfig struct {
	TablePathPrefix string
}

type service struct {
	database string
	db       ydb.Connection
}

func newService(ctx context.Context, opts ...ydb.Option) (s *service, err error) {
	db, err := ydb.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect error: %w", err)
	}
	s = &service{
		database: db.Name(),
		db:       db,
	}
	err = s.createTable(ctx)
	if err != nil {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("error on create table: %w", err)
	}
	return s, nil
}

func (s *service) Close(ctx context.Context) {
	defer func() { _ = s.db.Close(ctx) }()
}

func (s *service) createTable(ctx context.Context) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			CREATE TABLE urls (
				src Utf8,
				hash Utf8,

				PRIMARY KEY (hash)
			);
		`)),
		templateConfig{
			TablePathPrefix: s.database,
		},
	)
	return s.db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			err := s.ExecuteSchemeQuery(ctx, query)
			return err
		},
	)
}

func (s *service) insertShort(ctx context.Context, url string) (h string, err error) {
	h, err = hash(url)
	if err != nil {
		return "", err
	}
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $hash as Utf8;
			DECLARE $src as Utf8;

			REPLACE INTO
				urls (hash, src)
			VALUES
				($hash, $src);
		`)),
		templateConfig{
			TablePathPrefix: s.database,
		},
	)
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	err = s.db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				table.NewQueryParameters(
					table.ValueParam("$hash", types.UTF8Value(h)),
					table.ValueParam("$src", types.UTF8Value(url)),
				),
				options.WithCollectStatsModeBasic(),
			)
			return
		},
	)
	return h, err
}

func (s *service) selectLong(ctx context.Context, hash string) (url string, err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $hash as Utf8;

			SELECT
				src
			FROM
				urls
			WHERE
				hash = $hash;
		`)),
		templateConfig{
			TablePathPrefix: s.database,
		},
	)
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	var res result.Result
	err = s.db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$hash", types.UTF8Value(hash)),
				),
				options.WithQueryCachePolicy(
					options.WithQueryCachePolicyKeepInCache(),
				),
				options.WithCollectStatsModeBasic(),
			)
			return
		},
	)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = res.Close()
	}()
	var src string
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			err = res.ScanWithDefaults(&src)
			return src, err
		}
	}
	return "", fmt.Errorf(hashNotFound, hash)
}

func writeResponse(w http.ResponseWriter, statusCode int, body string) {
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(body))
}

func (s *service) Router(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimLeft(r.URL.Path, "/")
	switch {
	case path == "":
		http.ServeContent(w, r, "", time.Now(), bytes.NewReader([]byte(indexPageContent)))
	case path == "url":
		url := r.URL.Query().Get("url")
		if !isLongCorrect(url) {
			writeResponse(w, http.StatusBadRequest, fmt.Sprintf(invalidURLError, url))
			return
		}
		hash, err := s.insertShort(r.Context(), url)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/text")
		protocol := "https://"
		if r.TLS == nil {
			protocol = "http://"
		}
		writeResponse(w, http.StatusOK, protocol+r.Host+"/"+hash)
	default:
		if !isShortCorrect(path) {
			writeResponse(w, http.StatusBadRequest, fmt.Sprintf(invalidHashError, path))
			return
		}
		url, err := s.selectLong(r.Context(), path)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		http.Redirect(w, r, url, http.StatusSeeOther)
	}
}
