package kafka

import (
	"context"
	"log"

	"github.com/crystal/api/functions"
	"github.com/crystal/api/row"

	"github.com/segmentio/kafka-go"
)

type KafkaSource struct {
	reader       *kafka.Reader
	ctx          context.Context
	deserializer KafkaDeserializer
}

type KafkaDeserializer func(kafka.Message) (row.Row, bool)

func NewKafkaSource(brokers []string, topic, groupID string, deserializer KafkaDeserializer) *KafkaSource {
	return &KafkaSource{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		ctx: context.Background(),
	}
}

func (ks KafkaSource) Apply(source_channel chan row.Row, result_channel chan row.Row) {
	st := functions.SourceTransformation{
		SourceFunction: ks.next,
	}
	st.Apply(source_channel, result_channel)
}

func (ks *KafkaSource) next() (row.Row, bool) {
	kafka_message, err := ks.reader.ReadMessage(ks.ctx)
	if err != nil {
		log.Printf("Kafka read error: %v", err)
		return row.Row{}, false
	}
	row, ok := ks.deserializer(kafka_message)
	return row, ok
}

func (KafkaSource) IsResultStateless() bool {
	return true
}

func (k KafkaSource) GetName() string {
	return "KAFKA_SOURCE"
}
