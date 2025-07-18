package producer

import (
	"context"

	"github.com/crystal/api/functions"
	"github.com/crystal/api/row"

	"github.com/segmentio/kafka-go"
)

type KafkaSink struct {
	functions.SinkTransformation
	ctx        context.Context
	writer     *kafka.Writer
	Serializer KafkaSerializer
}

type KafkaSerializer func(row.Row) (kafka.Message, error)

func NewKafkaSink(brokers string, topic string, serializer KafkaSerializer) *KafkaSink {
	kafka_writer := KafkaSink{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
		ctx:        context.Background(),
		Serializer: serializer,
	}
	kafka_writer.SinkFunction = kafka_writer.write
	return &kafka_writer
}

func (ks *KafkaSink) Apply(source_channel chan row.Row, result_channel chan row.Row) {
	ks.SinkTransformation.Apply(source_channel, result_channel)
}

func (ks *KafkaSink) write(input row.Row) {
	serialized_message, err := ks.Serializer(input)
	if err != nil {
		panic(err)
	}
	err = ks.writer.WriteMessages(ks.ctx, serialized_message)
	if err != nil {
		panic(err)
	}
}

func (k KafkaSink) IsResultStateless() bool {
	return true
}

func (k KafkaSink) GetName() string {
	return "KAFKA_SINK"
}
