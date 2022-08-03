package topicreaderexamples

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func ReadMessagesWithCustomBatching(ctx context.Context, db ydb.Connection) {
	reader, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithBatchReadMinCount(1000),
	)

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch.Context(), batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}

func ProcessMessagesWithSyncCommit(ctx context.Context, db ydb.Connection) {
	reader, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithCommitMode(topicoptions.CommitModeSync),
	)
	defer func() {
		_ = reader.Close(ctx)
	}()

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch.Context(), batch)
		_ = reader.Commit(ctx, batch) // will wait response about commit from server
	}
}

func OwnReadProgressStorage(ctx context.Context, db ydb.Connection) {
	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
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
		batch, _ := reader.ReadMessageBatch(ctx)

		processBatch(batch.Context(), batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.Topic(),
			batch.PartitionID(),
			getEndOffset(batch),
		)
	}
}
