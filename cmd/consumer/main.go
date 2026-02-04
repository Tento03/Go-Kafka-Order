package consumer

import (
	"context"
	"encoding/json"
	"go-kafka-order/internal/config"
	"go-kafka-order/internal/kafka"
	"go-kafka-order/internal/model"
	"go-kafka-order/internal/worker"
	"log"
	"os"
	"os/signal"
	"syscall"

	k "github.com/segmentio/kafka-go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	reader := kafka.NewConsumer(config.Brokers, config.GroupId, config.OrdersTopic)
	defer reader.Close()

	retryProducer := kafka.NewProducer(config.Brokers, config.RetryTopic)
	dlqProducer := kafka.NewProducer(config.Brokers, config.DLQTopic)

	pool := worker.NewPool(3)
	pool.Run(ctx, 3)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			break
		}

		var o model.Order
		json.Unmarshal(msg.Value, &o)

		retryCount := kafka.GetRetryCount(msg.Headers)

		pool.Submit(worker.Job{
			Order: o, Ack: func(success bool) {
				if success {
					reader.CommitMessages(ctx, msg)
					log.Printf("Offset committed for %s", o.ID)
					return
				}

				retryCount++
				if retryCount <= 3 {
					log.Printf("Retry ke-%d for order %s", retryCount, o.ID)
					retryProducer.Publish(ctx, k.Message{
						Value:   msg.Value,
						Headers: kafka.SetRetryCount(retryCount),
					})
				} else {
					log.Printf("Sent to DLQ: %s", o.ID)
					dlqProducer.Publish(ctx, k.Message{Value: msg.Value})
				}

				reader.CommitMessages(ctx, msg)
			},
		})
	}
}
