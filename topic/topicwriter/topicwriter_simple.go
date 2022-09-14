package topicwriter

import (
	"bytes"
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func SendMessagesOneByOne(ctx context.Context, w *topicwriter.Writer) {
	data := []byte{1, 2, 3}
	mess := topicwriter.Message{Data: bytes.NewReader(data)}
	_ = w.Write(ctx, mess)
}

func SendGroupOfMessages(ctx context.Context, w *topicwriter.Writer) {
	data1 := []byte{1, 2, 3}
	data2 := []byte{4, 5, 6}
	mess1 := topicwriter.Message{Data: bytes.NewReader(data1)}
	mess2 := topicwriter.Message{Data: bytes.NewReader(data2)}

	_ = w.Write(ctx, mess1, mess2)
}
