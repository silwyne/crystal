package kafka

import (
	"context"
	"log"
	"process-engine/internals/functions"
	"sync"

	"github.com/segmentio/kafka-go"
)

type KafkaSource struct {
	reader *kafka.Reader
	ctx    context.Context
}

func NewKafkaSource(brokers []string, topic, groupID string) *KafkaSource {
	return &KafkaSource{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		ctx: context.Background(),
	}
}

func (s KafkaSource) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	st := functions.SourceTransformation{
		Function: s.readKafkaMessage,
	}
	return st.ExecuteTransformation(wg, source_channel)
}

func (k *KafkaSource) readKafkaMessage() (interface{}, bool) {
	m, err := k.reader.ReadMessage(k.ctx)
	if err != nil {
		log.Printf("Kafka read error: %v", err)
		return nil, false
	}
	return m, true
}
