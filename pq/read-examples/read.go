package read_examples

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/pq"
)

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
		_ = r.Commit(context.TODO(), mess.GetCommitOffset())
	}
}

func ReadMessageWithBatchCommit(r *pq.Reader) {
	var commits pq.CommitBatch
	defer func() {
		_ = r.CommitBatch(context.TODO(), commits)
	}()

	for {
		mess, _ := r.ReadMessage(context.TODO())
		processMessage(mess)

		commits.Append(mess)

		if len(commits) == 1000 {
			_ = r.CommitBatch(context.TODO(), commits)
			commits = pq.CommitBatch{}
		}
	}
}

func ReadBatchesWithBatchCommit(r *pq.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		processBatch(batch)
		_ = r.Commit(context.TODO(), batch.GetCommitOffset())
	}
}

func ReadBatchWithMessageCommits(r *pq.Reader) {
	for {
		batch, _ := r.ReadMessageBatch(context.TODO())
		for _, mess := range batch.Messages {
			processMessage(mess)
			_ = r.Commit(context.TODO())
		}
	}
}

func ReadWithGracefulShudownSession(r *pq.Reader) {
	sessions := map[*pq.PartitionSession][]pq.Message{}

	ensureSession := func(session *pq.PartitionSession) *pq.PartitionSession {
		if _, ok := sessions[session]; ok {
			return session
		}

		// Обработка на graceful shutdown
		go func() {
			select {
			case <-session.GracefulContext().Done():
				messages := sessions[session]
				processPartitionedMessages(session.Context(), messages)
				_ = r.CommitBatch(context.TODO(), pq.CommitBatchFromMessages(messages...))
			case <-session.Context().Done():
				return
			}
		}()

		return session
	}

	for {
		m, _ := r.ReadMessage(context.TODO())
		ensureSession(m.PartitionSession)
		messages := sessions[m.PartitionSession]
		messages = append(messages, m)
		if len(sessions[m.PartitionSession]) == 1000 {
			processPartitionedMessages(m.PartitionSession.Context(), messages)
			_ = r.CommitBatch(context.TODO(), pq.CommitBatchFromMessages(messages...))
			messages = messages[:0]
		}
		sessions[m.PartitionSession] = messages
	}
}

func processBatch(batch pq.Batch) {
	buf := &bytes.Buffer{}
	for _, mess := range batch.Messages {
		_, _ = buf.ReadFrom(mess.Data)
		writeBatchToDB(batch.Context(), batch.WriteTimestamp, buf.Bytes())
	}
}

func processMessage(m pq.Message) {
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
