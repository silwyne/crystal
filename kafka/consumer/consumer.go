package consumer

import (
	"context"

	"github.com/crystal/api/functions"
	"github.com/crystal/api/row"

	"github.com/segmentio/kafka-go"
)

type KafkaSource struct {
	functions.SourceTransformation
	reader       *kafka.Reader
	ctx          context.Context
	deserializer KafkaDeserializer
}

type KafkaDeserializer func(kafka.Message) (row.Row, error)

func NewKafkaSource(brokers []string, topic, groupID string, deserializer KafkaDeserializer) *KafkaSource {
	source := KafkaSource{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		ctx:          context.Background(),
		deserializer: deserializer,
	}
	source.SourceFunction = source.next
	return &source
}

func (ks *KafkaSource) Apply(source_channel chan row.Row, result_channel chan row.Row) {
	ks.SourceTransformation.Apply(source_channel, result_channel)
}

func (ks *KafkaSource) next() (row.Row, error) {
	kafka_message, err := ks.reader.ReadMessage(ks.ctx)
	if err != nil {
		panic(err)
	}
	row, err := ks.deserializer(kafka_message)
	if err != nil {
		panic(err)
	}
	return row, nil
}

func (k KafkaSource) IsResultStateless() bool {
	return true
}

func (k KafkaSource) GetName() string {
	return "KAFKA_SOURCE"
}
