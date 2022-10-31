package main

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func (s *dbServer) dropFromCache(id string) {
	_ = s.cache.Delete(id)
}

func (s *dbServer) cdcLoop() {
	ctx := context.Background()
	consumer := "consumer-" + strconv.Itoa(s.id)
	err := s.db.Topic().Alter(ctx, "bus/updates", topicoptions.AlterWithAddConsumers(topictypes.Consumer{
		Name: consumer,
	}))
	if err != nil {
		if !ydb.IsOperationErrorAlreadyExistsError(err) {
			log.Fatalf("failed to add consumer: %+v", err)
		}
	}

	reader, err := s.db.Topic().StartReader(consumer, topicoptions.ReadSelectors{
		{
			Path:     "bus/updates",
			ReadFrom: time.Now(),
		},
	},
	)
	if err != nil {
		log.Fatalf("failed to start reader: %+v", err)
	}

	log.Printf("Start cdc listen for server: %v", s.id)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("failed to read message: %+v", err)
		}

		var cdcEvent struct {
			Key    []string
			Update struct {
				Text string
			}
		}

		err = topicsugar.JSONUnmarshal(msg, &cdcEvent)
		if err != nil {
			log.Fatalf("failed to unmarshal message: %+v", err)
		}

		busID := cdcEvent.Key[0]
		// s.dropFromCache(articleID)
		s.storeInCache(busID, cdcEvent.Update.Text)
	}
}
