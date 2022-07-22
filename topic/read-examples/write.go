package readexamples

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func CreateWriter() *topicwriter.Writer {
	ctx := context.Background()
	db, _ := ydb.Open(
		ctx, "grpc://localhost:2136?database=/local",
		ydb.WithAccessTokenCredentials("..."),
	)

	// Simple
	w, _ := db.Topic().StartWriter("/database/topic", "producer-id")

	// WithSpecificCodec
	w, _ = db.Topic().StartWriter("/database/topic", "producer-id",
		topicoptions.WithCodec(topictypes.CodecGzip),
	)

	// WithMessageGroupID
	w, _ = db.Topic().StartWriter("/database/topic", "producer-id",
		topicoptions.WithMessageGroupID("asd"),
	)

	// SessionMetadata
	w, _ = db.Topic().StartWriter("/database/topic", "producer-id",
		topicoptions.WithWriteSessionMeta(topicoptions.WriteSessionMetadata{
			"sender-fqdn": "host-1",
			"key":         "val",
		}),
	)

	// WithRequestLastSeqNo from server
	w, _ = db.Topic().StartWriter("/database/topic", "prodicer-id",
		topicoptions.WithOnWriterConnected(func(info topicwriter.WithOnWriterConnectedInfo) error {
			fmt.Println(info.LastSeqNo)
			return nil
		}),
	)

	// With partitioning on writer level
	w, _ = db.Topic().StartWriter("/database/topic", "prodicer-id",
		topicoptions.WithWriterPartitioning(topicwriter.NewPartitioningWithPartitionID(4)),
	)

	// Wait ack from server before return from write
	w, _ = db.Topic().StartWriter("/database/topic", "producer-id",
		topicoptions.WithSyncWrite(true),
	)

	return w
}

func SimpleWriteMessages(ctx context.Context, w *topicwriter.Writer) {
	for i := 0; i < 100; i++ {
		m := topicwriter.Message{Data: strings.NewReader(fmt.Sprint(i))}
		_ = w.Write(ctx, m)
	}
}

func BatchedWriteMessages(ctx context.Context, w *topicwriter.Writer) {
	count := 100
	messages := make([]topicwriter.Message, 0, count)
	for i := 0; i < count; i++ {
		m := topicwriter.Message{Data: strings.NewReader(fmt.Sprint(i))}
		messages = append(messages, m)
	}
	_ = w.Write(ctx, messages...)
}

func WriteMessagesWithPartitioningOnMessageLevelByGroupID(ctx context.Context, w *topicwriter.Writer) {
	for i := 0; i < 100; i++ {
		grpID := strconv.Itoa(i)

		m := topicwriter.Message{
			Data:         strings.NewReader(fmt.Sprint(i)),
			Partitioning: topicwriter.NewPartitioningWithMessageGroupID(grpID),
		}
		_ = w.Write(ctx, m)
	}
}

func WriteMessagesWithPartitioningOnMessageLevelByPartitionID(ctx context.Context, w *topicwriter.Writer) {
	partitionCount := 3
	for i := 0; i < 100; i++ {
		partitionID := int64(i / 10 % partitionCount)

		m := topicwriter.Message{
			Data:         strings.NewReader(fmt.Sprint(i)),
			Partitioning: topicwriter.NewPartitioningWithPartitionID(partitionID),
		}
		_ = w.Write(ctx, m)
	}
}

func WithManualSequenceNumber(ctx context.Context, db ydb.Connection) {
	w, _ := db.Topic().StartWriter("path", "producer-id", topicoptions.WithWriterSetAutoSeqNo(false))
	for i := 0; i < 100; i++ {
		m := topicwriter.Message{
			SeqNo: int64(i),
			Data:  strings.NewReader(fmt.Sprint(i)),
		}
		_ = w.Write(ctx, m)
	}
}

func WithManualCreatedAt(ctx context.Context, w *topicwriter.Writer) {
	for i := 0; i < 100; i++ {
		m := topicwriter.Message{
			CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i),
			Data:      strings.NewReader(fmt.Sprint(i)),
		}
		_ = w.Write(ctx, m)
	}
}
