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
		_ = r.Commit(context.TODO(), mess)
	}
}

func ReadMessageWithBatchCommit(r *pq.Reader) {
	var processedMessages []*pq.Message
	defer func() {
		_ = r.CommitMessages(context.TODO(), processedMessages...)
	}()

	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)

		processedMessages = append(processedMessages, mess)

		if len(processedMessages) == 1000 {
			_ = r.CommitMessages(context.TODO(), processedMessages...)
			processedMessages = processedMessages[:0]
		}
	}
}

func ReadBatchesWithBatchCommit(r *pq.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		processBatch(batch.Context(), batch)
		_ = r.Commit(context.TODO(), batch)
	}
}

func ReadBatchWithMessageCommits(r *pq.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		for _, mess := range batch.Messages {
			processMessage(&mess)
			_ = r.Commit(context.TODO(), batch)
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
		_ = r.Commit(context.TODO(), batch)
	}
}

func ReadWithExplicitPartitionStartStopHandler1(r *pq.Reader) {
	r.OnStartPartition(func(ctx context.Context, req pq.OnStartPartitionRequest) (res pq.OnStartPartitionResponse, err error) {
		offset, _ := externalSystemLock(ctx, req.Session.Topic, req.Session.PartitionID)

		req.Session.OnGracefulStop(func(ctx context.Context, partition *pq.PartitionSession) error {
			return externalSystemUnlock(ctx, partition.Topic, partition.PartitionID)
		})

		res.SetReadOffset(offset)
		return res, nil
	})

	ctx := context.Background()
	for {
		batch, _ := r.ReadMessageBatch(ctx)

		processBatch(batch.Context(), batch)
		_ = externalSystemCommit(batch.Context(), batch.PartitionSession().Topic, batch.PartitionSession().PartitionID, batch.ToOffset.ToInt64())
	}
}

func ReadWithExplicitPartitionStartStopHandler2(r *pq.Reader) {
	r.OnStartPartition(func(ctx context.Context, req pq.OnStartPartitionRequest) (res pq.OnStartPartitionResponse, err error) {
		offset, _ := externalSystemLock(ctx, req.Session.Topic, req.Session.PartitionID)

		res.SetReadOffset(offset)
		return res, nil
	})

	r.OnStopPartition(func(ctx context.Context, req *pq.OnStopPartitionRequest) error {
		return externalSystemUnlock(ctx, req.Partition.Topic, req.Partition.PartitionID)
	})

	ctx := context.Background()
	for {
		batch, _ := r.ReadMessageBatch(ctx)

		processBatch(batch.Context(), batch)
		_ = externalSystemCommit(batch.Context(), batch.PartitionSession().Topic, batch.PartitionSession().PartitionID, batch.ToOffset.ToInt64())
	}
}

func processBatch(ctx context.Context, batch *pq.Batch) {
	if len(batch.Messages) == 0 {
		return
	}

	buf := &bytes.Buffer{}
	for _, mess := range batch.Messages {
		_, _ = buf.ReadFrom(mess.Data)
		writeBatchToDB(ctx, batch.Messages[0].WrittenAt, buf.Bytes())
	}
}

func processMessage(m *pq.Message) {
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

func externalSystemLock(ctx context.Context, topic string, partition int64) (offset int64, err error) {
	panic("not implemented")
}

func externalSystemUnlock(ctx context.Context, topic string, partition int64) error {
	panic("not implemented")
}

func externalSystemCommit(ctx context.Context, topic string, partition int64, offset int64) error {
	panic("not implemented")
}
