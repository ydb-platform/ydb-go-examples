package read_examples

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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
		processBatch(batch)
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
		processBatch(batch)
		_ = r.Commit(context.TODO(), batch)
	}
}

func ReadWithOwnReadProgressStorage(ctx context.Context, db ydb.Connection) {
	r := db.Topic().Reader(ctx,
		topic.WithReadSelector(topic.ReadSelector{Stream: "asd"}),
		topic.WithGetPartitionStartOffset(func(ctx context.Context, req topic.GetPartitionStartOffsetRequest) (res topic.GetPartitionStartOffsetResponse, err error) {
			offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
			res.StartWithAutoCommitFrom(offset)

			// Reader will stop if return err != nil
			return res, err
		}),
	)

	for {
		batch, _ := r.ReadMessageBatch(ctx)

		processBatch(batch)
		_ = externalSystemCommit(batch.Context(), batch.PartitionSession().Topic, batch.PartitionSession().PartitionID, batch.ToOffset.ToInt64())
	}
}

func ReadWithExplicitPartitionStartStopHandler(ctx context.Context, db ydb.Connection) {
	readContext, stopReader := context.WithCancel(context.Background())

	r := db.Topic().Reader(ctx,
		topic.WithReadSelector(topic.ReadSelector{Stream: "asd"}),
		topic.WithBaseContext(readContext), // cancel the context mean code can't continue to work. It will close reader and cancel context of all partitions
		topic.WithTracer(
			trace.TopicReader{
				OnPartitionReadStart: func(info trace.OnPartitionReadStartInfo) {
					err := externalSystemLock(info.PartitionContext, info.Topic, info.PartitionID)
					if err != nil {
						stopReader()
					}
				},
				OnPartitionReadStop: func(info trace.OnPartitionReadStopInfo) {
					if info.Graceful {
						err := externalSystemUnlock(ctx, info.Topic, info.PartitionID)
						if err != nil {
							stopReader()
						}
					}
				},
			},
		),
	)

	for {
		batch, _ := r.ReadMessageBatch(readContext)

		processBatch(batch)
		_ = externalSystemCommit(batch.Context(), batch.PartitionSession().Topic, batch.PartitionSession().PartitionID, batch.ToOffset.ToInt64())
	}
}

func ReceiveCommitNotify(db ydb.Connection) {
	ctx := context.Background()

	r := db.Topic().Reader(ctx,
		topic.WithReadSelector(topic.ReadSelector{Stream: "asd"}),
		topic.WithTracer(trace.TopicReader{
			OnPartitionCommittedNotify: func(info trace.OnPartitionCommittedInfo) {
				// called when receive commit notify from server
				fmt.Println(info.Topic, info.PartitionID, info.CommittedOffset)
			},
		},
		),
	)

	for {
		mess, _ := r.ReadMessage(ctx)
		processMessage(mess)
	}
}

func processBatch(batch *topic.Batch) {
	ctx := batch.Context() // batch.Context() will cancel if partition revoke by server or connection broke
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
	writeToDB(
		m.Context(), // m.Context will skip if server revoke partition or connection to server broken
		m.SeqNo, body)
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

func externalSystemLock(ctx context.Context, topic string, partition int64) (err error) {
	panic("not implemented")
}

func readLastOffsetFromDB(ctx context.Context, topic string, partition int64) (int64, error) {
	panic("not implemented")
}

func externalSystemUnlock(ctx context.Context, topic string, partition int64) error {
	panic("not implemented")
}

func externalSystemCommit(ctx context.Context, topic string, partition int64, offset int64) error {
	panic("not implemented")
}
