package topicreaderexamples

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

func PrintMessageContent(ctx context.Context, reader *topicreader.Reader) {
	for {
		msg, _ := reader.ReadMessage(ctx)
		content, _ := ioutil.ReadAll(msg)
		fmt.Println(string(content))
		_ = reader.Commit(msg.Context(), msg)
	}
}

func UnmarshalMessageContentToJSONStruct(msg *topicreader.Message) {
	type S struct {
		MyField int `json:"my_field"`
	}

	var v S

	_ = topicsugar.JSONUnmarshal(msg, &v)
}
