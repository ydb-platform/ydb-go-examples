package read_examples

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

func CreateReader() *topic.Reader {
	ctx := context.Background()
	db, _ := ydb.Open(
		ctx, "grpc://localhost:2136?database=/local",
		ydb.WithAccessTokenCredentials("..."),
	)

	r := db.Topic().Reader(context.TODO(),
		// The context will use as base to create PartitionSession context
		// Similar to http.Server.BaseContext
		// optional, if skip - context.Background will use as base
		topic.WithBaseContext(ctx),
		topic.WithReadSelector(topic.ReadSelector{
			Stream:             "test",
			Partitions:         nil, // по умолчанию - все
			SkipMessagesBefore: time.Time{},
		}),
		topic.WithReadSelector(topic.ReadSelector{
			Stream:             "test-2",
			Partitions:         []int64{1, 2, 3},
			SkipMessagesBefore: time.Time{},
		}),
	)
	return r
}

func SimpleReadMessages(r *topic.Reader) {
	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)
	}
}

func ReadWithCommitEveryMessage(r *topic.Reader) {
	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)
		_ = r.Commit(context.TODO(), mess)
	}
}

func ReadMessageWithBatchCommit(r *topic.Reader) {
	var processedMessages []*topic.Message
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

func ReadBatchesWithBatchCommit(r *topic.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		processBatch(batch.Context(), batch)
		_ = r.Commit(context.TODO(), batch)
	}
}

func ReadBatchWithMessageCommits(r *topic.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		for _, mess := range batch.Messages {
			processMessage(&mess)
			_ = r.Commit(context.TODO(), batch)
		}
	}
}

func ReadMessagedWithCustomBatching(db ydb.Connection) {
	r := db.Topic().Reader(context.TODO(),
		topic.WithBatchPreferCount(1000),
		topic.WithBatchMaxTimeLag(time.Second),
	)

	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		processBatch(batch.Context(), batch)
		_ = r.Commit(context.TODO(), batch)
	}
}

func ReadWithExplicitPartitionStartStopHandler(db ydb.Connection) {
	ctx := context.Background()

	stopPartitionHandler := func(ctx context.Context, req *topic.OnStopPartitionRequest) error {
		return externalSystemUnlock(ctx, req.Partition.Topic, req.Partition.PartitionID)
	}

	r := db.Topic().Reader(ctx,
		topic.WithPartitionStartHandler(func(ctx context.Context, req topic.OnStartPartitionRequest) (res topic.OnStartPartitionResponse, err error) {
			offset, _ := externalSystemLock(ctx, req.Session.Topic, req.Session.PartitionID)

			res.StartReadFrom(offset)
			return res, nil
		}),
		topic.WithPartitionStopHandler(stopPartitionHandler),
	)

	for {
		batch, _ := r.ReadMessageBatch(ctx)

		processBatch(batch.Context(), batch)
		_ = externalSystemCommit(batch.Context(), batch.PartitionSession().Topic, batch.PartitionSession().PartitionID, batch.ToOffset.ToInt64())
	}
}

func ReceiveCommitNotify(db ydb.Connection) {
	ctx := context.Background()

	r := db.Topic().Reader(ctx,
		topic.WithNotifyAcceptedCommit(func(req topic.OnCommitAcceptedRequest) {
			fmt.Println(req.PartitionSession.Topic, req.PartitionSession.PartitionID, req.ComittedOffset)
		}),
	)

	for {
		mess, _ := r.ReadMessage(ctx)
		processMessage(mess)
	}
}

func CommitSkippedStartOffsets(db ydb.Connection) {
	ctx := context.Background()

	// 1
	var partSession *topic.PartitionSession
	r := db.Topic().Reader(ctx, topic.WithPartitionStartHandler(func(ctx context.Context, req topic.OnStartPartitionRequest) (res topic.OnStartPartitionResponse, err error) {
		partSession = req.Session
		return res, nil
	}))
	partSession.CommitSkipped(from, to)

	// 2
	r.GetSessionForPartition(partNumber).Commit(from, to)

	// 3
	r.CommitSkipped(partNumber, from, to)
}

func processBatch(ctx context.Context, batch *topic.Batch) {
	if len(batch.Messages) == 0 {
		return
	}

	buf := &bytes.Buffer{}
	for _, mess := range batch.Messages {
		_, _ = buf.ReadFrom(mess.Data)
		writeBatchToDB(ctx, batch.Messages[0].WrittenAt, buf.Bytes())
	}
}

func processMessage(m *topic.Message) {
	body, _ := io.ReadAll(m.Data)
	writeToDB(m.Context(), m.SeqNo, body)
}

func processPartitionedMessages(ctx context.Context, messages []topic.Message) {
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
