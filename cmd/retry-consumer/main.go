package retryconsumer

import (
	"context"
	"go-kafka-order/internal/config"
	"go-kafka-order/internal/kafka"
	"log"
	"time"

	k "github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	reader := kafka.NewConsumer(config.Brokers, config.GroupId, config.OrdersTopic)
	producer := kafka.NewProducer(config.Brokers, config.OrdersTopic)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			break
		}

		log.Println("Retry consumer delay")
		time.Sleep(3 * time.Second)

		producer.Publish(ctx, k.Message{
			Value:   msg.Value,
			Headers: msg.Headers,
		})
		reader.CommitMessages(ctx, msg)
	}
}
