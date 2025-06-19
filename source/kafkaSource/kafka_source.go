package kafkaSource

import (
	"context"
	"log"
	"process-engine/api/functions"
	"sync"

	"github.com/segmentio/kafka-go"
)

type KafkaSourceTransformation struct {
	Function functions.SourceFunction
	reader   *kafka.Reader
	ctx      context.Context
}

func NewKafkaSource(brokers []string, topic, groupID string) *KafkaSourceTransformation {
	return &KafkaSourceTransformation{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		ctx: context.Background(),
	}
}

func (s KafkaSourceTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	st := functions.SourceTransformation{
		Function: s.next,
	}
	return st.ExecuteTransformation(wg, source_channel)
}

func (k *KafkaSourceTransformation) next() (interface{}, bool) {
	m, err := k.reader.ReadMessage(k.ctx)
	if err != nil {
		log.Printf("Kafka read error: %v", err)
		return nil, false
	}
	return m, true
}

func (k KafkaSourceTransformation) GetName() string {
	return "KafkaSourceTransformation"
}
