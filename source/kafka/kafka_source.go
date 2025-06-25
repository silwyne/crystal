package kafka

import (
	"context"
	"log"
	"sync"

	"github.com/crystal/api/functions"

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

func (ks KafkaSource) Execute(wg *sync.WaitGroup, source_channel chan interface{}) chan kafka.Message {
	st := functions.SourceTransformation[any, kafka.Message]{
		SourceFunction: ks.next,
	}
	return st.Execute(wg, source_channel)
}

func (ks *KafkaSource) next() (kafka.Message, bool) {
	m, err := ks.reader.ReadMessage(ks.ctx)
	if err != nil {
		log.Printf("Kafka read error: %v", err)
		return m, false
	}
	return m, true
}

func (k KafkaSource) GetName() string {
	return "KAFKA_SOURCE"
}
