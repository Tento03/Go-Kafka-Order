package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return &Producer{w}
}

func (p *Producer) Publish(ctx context.Context, msg kafka.Message) error {
	err := p.writer.WriteMessages(ctx, msg)
	if err == nil {
		log.Printf("Message published | topic=%s partition=%d offset=%d",
			p.writer.Topic, msg.Partition, msg.Offset)
	}
	return err
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
