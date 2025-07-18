package consumer

import (
	"context"
	"log"

	"github.com/crystal/api/row"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaSource struct {
	client       *kgo.Client
	ctx          context.Context
	deserializer KafkaDeserializer
}

type KafkaDeserializer func(*kgo.Record) (row.Row, error)

func NewKafkaSource(brokers string, topic string, groupID string, deserializer KafkaDeserializer) *KafkaSource {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(groupID),
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("unable to create kafka client: %v", err)
	}

	source := KafkaSource{
		client:       client,
		ctx:          context.Background(),
		deserializer: deserializer,
	}
	return &source
}

func (ks *KafkaSource) Apply(source_channel chan row.Row, result_channel chan row.Row) {

	for {
		fetches := ks.client.PollFetches(ks.ctx)
		if fetches.Err() != nil {
			log.Panicf("Error fetching records: %v \n", fetches.Err())
			panic(fetches.Err())
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			deserialized_m, err := ks.deserializer(record)
			if err != nil {
				log.Panicf("Error Deserializing KafkaMessage: %v, Error: %v \n", record, err)
				panic(fetches.Err())
			}
			result_channel <- deserialized_m
		}
	}
}

func (k KafkaSource) IsResultStateless() bool {
	return true
}

func (k KafkaSource) GetName() string {
	return "KAFKA_SOURCE"
}
