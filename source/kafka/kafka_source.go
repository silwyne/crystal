package kafka

import (
	"context"
	"log"
	"process-engine/api/functions"
	"process-engine/api/operation"
	"sync"

	"github.com/segmentio/kafka-go"
)

type KafkaSource struct {
	Function functions.SourceFunction
	reader   *kafka.Reader
	ctx      context.Context
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

func (ks KafkaSource) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	st := functions.SourceTransformation{
		Function: ks.next,
	}
	return st.ExecuteTransformation(wg, source_channel)
}

func (ks *KafkaSource) next() (interface{}, bool) {
	m, err := ks.reader.ReadMessage(ks.ctx)
	if err != nil {
		log.Printf("Kafka read error: %v", err)
		return nil, false
	}
	return m, true
}

func (k KafkaSource) GetTransformationType() operation.TransformationType {
	return operation.SOURCE
}
