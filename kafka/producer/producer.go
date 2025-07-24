package producer

import (
	"context"
	"log"

	"github.com/crystal/api/operation/signal"
	"github.com/crystal/api/row"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaSink struct {
	ctx        context.Context
	client     *kgo.Client
	Serializer KafkaSerializer
}

type KafkaSerializer func(row.Row) (*kgo.Record, error)

func NewKafkaSink(brokers string, topic string, serializer KafkaSerializer) *KafkaSink {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
		kgo.DefaultProduceTopic(topic),
		kgo.ClientID("crystal-sink-id"),
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("unable to create kafka client: %v", err)
	}

	kafka_writer := KafkaSink{
		client:     client,
		ctx:        context.Background(),
		Serializer: serializer,
	}

	return &kafka_writer
}

func (ks *KafkaSink) Apply(source_channel chan row.Row, result_channel chan row.Row) signal.Signal {
	for input := range source_channel {
		serialized_message, err := ks.Serializer(input)
		if err != nil {
			log.Fatalf("failed to serialize message: %v", err)
			return signal.FAILURE
		}

		var isFailed bool = false
		ks.client.Produce(ks.ctx, serialized_message, func(r *kgo.Record, err error) {
			if err != nil {
				log.Fatalf("failed to produce message: %v", err)
				isFailed = true
			}
		})
		if isFailed {
			return signal.FAILURE
		}
	}
	return signal.SUCCESS
}

func (k KafkaSink) IsResultStateless() bool {
	return true
}

func (k KafkaSink) GetName() string {
	return "KAFKA_SINK"
}
