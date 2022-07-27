package readexamples

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func CreateReader() *topicreader.Reader {
	ctx := context.Background()

	db, _ := ydb.Open(
		ctx, "grpc://localhost:2136?database=/local",
		ydb.WithAccessTokenCredentials("..."),
	)

	r, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"))
	return r
}

func CreateReaderWithManyTopicsAndOptions(db ydb.Connection) *topicreader.Reader {
	r, _ := db.Topic().StartReader("consumer", []topicoptions.ReadSelector{
		{
			Path: "test",
		},
		{
			Path:       "test-2",
			Partitions: []int64{1, 2, 3},
			ReadFrom:   time.Date(2022, 7, 1, 10, 15, 0, 0, time.UTC),
		},
	},
	)
	return r
}

func CreateReaderWithCustomCodec(db ydb.Connection) *topicreader.Reader {
	const CustomFlateCodec = topictypes.Codec(10001)

	r, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		// Add custom codec
		topicoptions.WithAddDecoder(CustomFlateCodec, func(input io.Reader) (io.Reader, error) {
			return flate.NewReader(input), nil
		}),
	)
	return r

}

func SimpleReadMessagesWithErrorHandle(ctx context.Context, r *topicreader.Reader) error {
	for {
		mess, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		processMessage(mess)
	}
}

func SimpleReadJSONMessageOptimized(ctx context.Context, r *topicreader.Reader) {
	type S struct {
		V int
	}

	var v S
	mess, _ := r.ReadMessage(ctx)
	_ = topicsugar.JSONUnmarshal(mess, &v)
}

type MyMessage struct {
	ID         byte
	ChangeType byte
	Delta      uint32
}

func (m *MyMessage) UnmarshalYDBTopicMessage(data []byte) error {
	if len(data) != 6 {
		return errors.New("bad data len")
	}
	m.ID = data[0]
	m.ChangeType = data[1]
	m.Delta = binary.BigEndian.Uint32(data[2:])
	return nil
}

func EffectiveReadMessageToOwnType(ctx context.Context, r *topicreader.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(ctx)
		results := make([]MyMessage, len(batch.Messages))
		for i := range results {
			_ = batch.Messages[i].UnmarshalTo(&results[i])
		}
		processResults(results)
		_ = r.Commit(ctx, batch)
	}
}

func processResults(_ []MyMessage) {

}

func HandlePartitionHardOff(batch *topicreader.Batch) {
	ctx := batch.Context() // batch.Context() will cancel if partition revoke by server or connection broke
	if len(batch.Messages) == 0 {
		return
	}

	buf := &bytes.Buffer{}
	for _, mess := range batch.Messages {
		if ctx.Err() != nil {
			return
		}
		_, _ = buf.ReadFrom(mess)
		writeBatchToDB(ctx, batch.Messages[0].WrittenAt, buf.Bytes())
	}
}

func HandlePartitionSoftOff(ctx context.Context, db ydb.Connection) {
	r, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithBatchReadMinCount(1000),
	)

	for {
		batch, _ := r.ReadMessageBatch(ctx) // <- if partition soft stop batch can be less, then 1000
		processBatch(batch)
		_ = r.Commit(batch.Context(), batch)
	}
}

func SimplePrintMessageContent(ctx context.Context, r *topicreader.Reader) {
	for {
		mess, _ := r.ReadMessage(ctx)
		content, _ := io.ReadAll(mess)
		fmt.Println(string(content))
	}
}

func ReadWithCommitEveryMessage(ctx context.Context, r *topicreader.Reader) {
	for {
		mess, _ := r.ReadMessage(ctx)
		processMessage(mess)
		_ = r.Commit(mess.Context(), mess)
	}
}

func ReadMessagesWithAsyncBufferedCommit(ctx context.Context, db ydb.Connection) {
	r, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithCommitMode(topicoptions.CommitModeAsync),
		topicoptions.WithCommitCountTrigger(1000),
	)
	defer func() {
		_ = r.Close(ctx) // wait until flush buffered commits
	}()

	for {
		mess, _ := r.ReadMessage(ctx)
		processMessage(mess)
		_ = r.Commit(ctx, mess) // will fast - in async mode commit will append to internal buffer only
	}
}

func ReadBatchesWithBatchCommit(ctx context.Context, r *topicreader.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(ctx)
		processBatch(batch)
		_ = r.Commit(batch.Context(), batch)
	}
}

func ReadBatchWithMessageCommits(ctx context.Context, r *topicreader.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(ctx)
		for _, mess := range batch.Messages {
			processMessage(mess)
			_ = r.Commit(mess.Context(), batch)
		}
	}
}

func ReadMessagesWithCustomBatching(ctx context.Context, db ydb.Connection) {
	r, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithBatchReadMinCount(1000),
	)

	for {
		batch, _ := r.ReadMessageBatch(ctx)
		processBatch(batch)
		_ = r.Commit(batch.Context(), batch)
	}
}

func ReadWithOwnReadProgressStorage(ctx context.Context, db ydb.Connection) {
	r, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithGetPartitionStartOffset(
			func(
				ctx context.Context,
				req topicoptions.GetPartitionStartOffsetRequest,
			) (
				res topicoptions.GetPartitionStartOffsetResponse,
				err error,
			) {
				offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
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
			batch.Topic(),
			batch.PartitionID(),
			getEndOffset(batch),
		)
	}
}

func ReadWithExplicitPartitionStartStopHandler(ctx context.Context, db ydb.Connection) {
	readContext, stopReader := context.WithCancel(context.Background())
	defer stopReader()

	r, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithTracer(
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
		_ = r.Close(ctx)
	}()

	for {
		batch, _ := r.ReadMessageBatch(readContext)

		processBatch(batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.Topic(),
			batch.PartitionID(),
			getEndOffset(batch),
		)
	}
}

func ReadWithExplicitPartitionStartStopHandlerAndOwnReadProgressStorage(ctx context.Context, db ydb.Connection) {
	readContext, stopReader := context.WithCancel(context.Background())
	defer stopReader()

	readStartPosition := func(
		ctx context.Context,
		req topicoptions.GetPartitionStartOffsetRequest,
	) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
		offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
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

	r, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),

		topicoptions.WithGetPartitionStartOffset(readStartPosition),
		topicoptions.WithTracer(
			trace.Topic{
				OnPartitionReadStart: onPartitionStart,
				OnPartitionReadStop:  onPartitionStop,
			},
		),
	)
	go func() {
		<-readContext.Done()
		_ = r.Close(ctx)
	}()

	for {
		batch, _ := r.ReadMessageBatch(readContext)

		processBatch(batch)
		_ = externalSystemCommit(batch.Context(), batch.Topic(), batch.PartitionID(), getEndOffset(batch))
	}
}

func ReceiveCommitNotify(db ydb.Connection) {
	ctx := context.Background()

	r, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithTracer(trace.Topic{
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

func processBatch(batch *topicreader.Batch) {
	ctx := batch.Context() // batch.Context() will cancel if partition revoke by server or connection broke
	if len(batch.Messages) == 0 {
		return
	}

	buf := &bytes.Buffer{}
	for _, mess := range batch.Messages {
		buf.Reset()
		_, _ = buf.ReadFrom(mess)
		_, _ = io.Copy(buf, mess)
		writeMessagesToDB(ctx, buf.Bytes())
	}
}

func processMessage(m *topicreader.Message) {
	body, _ := io.ReadAll(m)
	writeToDB(
		m.Context(), // m.Context will skip if server revoke partition or connection to server broken
		m.SeqNo, body)
}

func processPartitionedMessages(ctx context.Context, messages []topicreader.Message) {
	buf := &bytes.Buffer{}
	for _, mess := range messages {
		_, _ = buf.ReadFrom(&mess)
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

func getEndOffset(b *topicreader.Batch) int64 {
	panic("not implemented")
}
