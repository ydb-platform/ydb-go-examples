package write_examples

import (
	"context"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
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

	// WithConnectionCallback
	w, _ = db.Topic().StartWriter("/database/topic", "producer-id",
		topicoptions.WithMessageGroupID("asd"),
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
