package read_examples

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/pq"
)

func CreateReader() *pq.Reader {
	ctx := context.Background()
	db, _ := ydb.Open(
		ctx, "grpc://localhost:2136?database=/local",
		ydb.WithAccessTokenCredentials("..."),
	)

	r := db.Persqueue().Reader(context.TODO(),
		// The context will use as base to create PartitionSession context
		// Similar to http.Server.BaseContext
		// optional, if skip - context.Background will use as base
		pq.WithBaseContext(ctx),
		pq.WithReadSelector(pq.ReadSelector{
			Stream:             "test",
			Partitions:         nil, // по умолчанию - все
			SkipMessagesBefore: time.Time{},
		}),
		pq.WithReadSelector(pq.ReadSelector{
			Stream:             "test-2",
			Partitions:         []int64{1, 2, 3},
			SkipMessagesBefore: time.Time{},
		}),
	)
	return r
}

func SimpleReadMessages(r *pq.Reader) {
	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)
	}
}

func ReadWithCommitEveryMessage(r *pq.Reader) {
	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)
		_ = r.CommitMessage(context.TODO(), mess)
	}
}

func ReadMessageWithBatchCommit(r *pq.Reader) {
	var processedMessages []pq.Message
	defer func() {
		_ = r.CommitMessage(context.TODO(), processedMessages...)
	}()

	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)

		processedMessages = append(processedMessages, mess)

		if len(processedMessages) == 1000 {
			_ = r.CommitMessage(context.TODO(), processedMessages...)
			processedMessages = processedMessages[:0]
		}
	}
}

func ReadBatchesWithBatchCommit(r *pq.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		processBatch(batch.Context(), batch)
		_ = r.CommitBatch(context.TODO(), batch)
	}
}

func ReadBatchWithMessageCommits(r *pq.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		for _, mess := range batch.Messages {
			processMessage(mess)
			_ = r.CommitBatch(context.TODO(), batch)
		}
	}
}

func ReadBatchingOnSDKSideShudownSession(db ydb.Connection) {
	r := db.Persqueue().Reader(context.TODO(),
		pq.WithBatchPreferCount(1000),
	)

	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		processBatch(batch.Context(), batch)
		r.CommitBatch(context.TODO(), batch)
	}
}

func processBatch(ctx context.Context, batch pq.Batch) {
	if len(batch.Messages) == 0 {
		return
	}

	buf := &bytes.Buffer{}
	for _, mess := range batch.Messages {
		_, _ = buf.ReadFrom(mess.Data)
		writeBatchToDB(ctx, batch.Messages[0].WrittenAt, buf.Bytes())
	}
}

func processMessage(m pq.Message) {
	body, _ := io.ReadAll(m.Data)
	writeToDB(m.Context(), m.SeqNo, body)
}

func processPartitionedMessages(ctx context.Context, messages []pq.Message) {
	buf := &bytes.Buffer{}
	for _, mess := range messages {
		_, _ = buf.ReadFrom(mess.Data)
		writeMessagesToDB(ctx, buf.Bytes())
	}
}

func writeToDB(ctx context.Context, id int64, body []byte) {
}

func writeBatchToDB(ctx context.Context, t time.Time, data []byte) {

}

func writeMessagesToDB(ctx context.Context, data []byte) {}
