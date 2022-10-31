package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

func (s *dbServer) dropFromCache(id string) {
	s.cache.Delete(id)
}

func (s *dbServer) cdcLoop() {
	ctx := context.Background()
	consumer := consumerName(s.id)
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
				FreeSeats int64
			}
		}

		err = topicsugar.JSONUnmarshal(msg, &cdcEvent)
		if err != nil {
			log.Fatalf("failed to unmarshal message: %+v", err)
		}

		busID := cdcEvent.Key[0]
		s.dropFromCache(busID)
		// s.cache.Set(busID, cdcEvent.Update.FreeSeats)
	}
}

func consumerName(index int) string {
	return fmt.Sprintf("consumer-%v", index)
}
