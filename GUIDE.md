# Kafka Source 
```
import (
	"github.com/crystal/source/kafka_source"
	"github.com/segmentio/kafka-go"
)

deserializer := func(in kafka.Message) (row.Row, bool) {
    return row.From(string(in.Value)), true
}

source := kafka_source.NewKafkaSource(
    []string{"localhost:9092"},
    "test-0",
    "consumer-group-id",
    deserializer,
)

stream := streamEnv.FromSource(source)
```