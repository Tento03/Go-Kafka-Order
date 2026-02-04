package dlqconsumer

import (
	"context"
	"go-kafka-order/internal/config"
	"go-kafka-order/internal/kafka"
	"log"
)

func main() {
	ctx := context.Background()

	reader := kafka.NewConsumer(config.Brokers, config.GroupId, config.OrdersTopic)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			break
		}

		log.Printf("DQL message: %s", string(msg.Value))
		reader.CommitMessages(ctx, msg)
	}
}
