package readexamples

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func CreateReader() *topicreader.Reader {
	ctx := context.Background()
	db, _ := ydb.Open(
		ctx, "grpc://localhost:2136?database=/local",
		ydb.WithAccessTokenCredentials("..."),
	)

	r := db.Topic().Reader(context.TODO(),
		// The context will use as base to create PartitionSession context
		// Similar to http.Server.BaseContext
		// optional, if skip - context.Background will use as base
		topicreader.WithBaseContext(ctx),
		topicreader.WithReadSelector(topicreader.ReadSelector{
			Stream:     "test",
			Partitions: nil, // по умолчанию - все
			ReadFrom:   time.Time{},
		}),
		topicreader.WithReadSelector(topicreader.ReadSelector{
			Stream:     "test-2",
			Partitions: []int64{1, 2, 3},
			ReadFrom:   time.Time{},
		}),
	)
	return r
}

func SimpleReadMessages(r *topicreader.Reader) {
	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)
	}
}

func ReadWithCommitEveryMessage(r *topicreader.Reader) {
	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)
		_ = r.Commit(context.TODO(), mess)
	}
}

func ReadMessageWithBatchCommit(ctx context.Context, db ydb.Connection) {
	r := db.Topic().Reader(ctx,
		topicreader.WithCommitMode(topicreader.CommitModeAsync),
		topicreader.WithCommitCountTrigger(1000),
	)
	defer func() {
		_ = r.Close() // wait until flush buffered commits
	}()

	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)
		_ = r.Commit(ctx, mess) // will fast - in async mode commit will append to internal buffer only
	}
}

func ReadBatchesWithBatchCommit(r *topicreader.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		processBatch(batch)
		_ = r.Commit(context.TODO(), batch)
	}
}

func ReadBatchWithMessageCommits(r *topicreader.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		for _, mess := range batch.Messages {
			processMessage(mess)
			_ = r.Commit(context.TODO(), batch)
		}
	}
}

func ReadMessagedWithCustomBatching(db ydb.Connection) {
	r := db.Topic().Reader(context.TODO(),
		topicreader.WithBatchReadOptions(topicreader.WithBatchMinCount(1000)),
		topicreader.WithBatchMaxTimeLag(time.Second),
	)

	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		processBatch(batch)
		_ = r.Commit(context.TODO(), batch)
	}
}

func ReadWithOwnReadProgressStorage(ctx context.Context, db ydb.Connection) {
	r := db.Topic().Reader(ctx,
		topicreader.WithReadSelector(topicreader.ReadSelector{Stream: "asd"}),
		topicreader.WithGetPartitionStartOffset(
			func(
				ctx context.Context,
				req topicreader.GetPartitionStartOffsetRequest,
			) (
				res topicreader.GetPartitionStartOffsetResponse,
				err error,
			) {
				offset, err := readLastOffsetFromDB(ctx, req.Session.Topic, req.Session.PartitionID)
				res.StartFrom(offset)

				// Reader will stop if return err != nil
				return res, err
			}),
	)

	for {
		batch, _ := r.ReadMessageBatch(ctx)

		processBatch(batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.PartitionSession().Topic,
			batch.PartitionSession().PartitionID,
			batch.EndOffset(),
		)
	}
}

func ReadWithExplicitPartitionStartStopHandler(ctx context.Context, db ydb.Connection) {
	readContext, stopReader := context.WithCancel(context.Background())
	defer stopReader()

	r := db.Topic().Reader(ctx,
		topicreader.WithReadSelector(topicreader.ReadSelector{Stream: "asd"}),
		topicreader.WithTracer(
			trace.Topic{
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

	go func() {
		<-readContext.Done()
		_ = r.Close()
	}()

	for {
		batch, _ := r.ReadMessageBatch(readContext)

		processBatch(batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.PartitionSession().Topic,
			batch.PartitionSession().PartitionID,
			batch.EndOffset(),
		)
	}
}

func ReadWithExplicitPartitionStartStopHandlerAndOwnReadProgressStorage(ctx context.Context, db ydb.Connection) {
	readContext, stopReader := context.WithCancel(context.Background())
	defer stopReader()

	readStartPosition := func(
		ctx context.Context,
		req topicreader.GetPartitionStartOffsetRequest,
	) (res topicreader.GetPartitionStartOffsetResponse, err error) {
		offset, err := readLastOffsetFromDB(ctx, req.Session.Topic, req.Session.PartitionID)
		res.StartFrom(offset)

		// Reader will stop if return err != nil
		return res, err
	}

	onPartitionStart := func(info trace.OnPartitionReadStartInfo) {
		err := externalSystemLock(info.PartitionContext, info.Topic, info.PartitionID)
		if err != nil {
			stopReader()
		}
	}

	onPartitionStop := func(info trace.OnPartitionReadStopInfo) {
		if info.Graceful {
			err := externalSystemUnlock(ctx, info.Topic, info.PartitionID)
			if err != nil {
				stopReader()
			}
		}
	}

	r := db.Topic().Reader(ctx,
		topicreader.WithReadSelector(topicreader.ReadSelector{Stream: "asd"}),

		// all partition contexts based on base context and will cancel with readContext
		topicreader.WithBaseContext(readContext),
		topicreader.WithGetPartitionStartOffset(readStartPosition),
		topicreader.WithTracer(
			trace.Topic{
				OnPartitionReadStart: onPartitionStart,
				OnPartitionReadStop:  onPartitionStop,
			},
		),
	)
	go func() {
		<-readContext.Done()
		_ = r.Close()
	}()

	for {
		batch, _ := r.ReadMessageBatch(readContext)

		processBatch(batch)
		_ = externalSystemCommit(batch.Context(), batch.PartitionSession().Topic, batch.PartitionSession().PartitionID, batch.EndOffset())
		r.Commit(ctx, batch)
	}
}

func ReceiveCommitNotify(db ydb.Connection) {
	ctx := context.Background()

	r := db.Topic().Reader(ctx,
		topicreader.WithReadSelector(topicreader.ReadSelector{Stream: "asd"}),
		topicreader.WithTracer(trace.Topic{
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

func processBatch(batch topicreader.Batch) {
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

func processMessage(m topicreader.Message) {
	body, _ := io.ReadAll(m.Data)
	writeToDB(
		m.Context(), // m.Context will skip if server revoke partition or connection to server broken
		m.SeqNo, body)
}

func processPartitionedMessages(ctx context.Context, messages []topicreader.Message) {
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
