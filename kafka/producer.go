package kafka

import (
	"context"
	"log"

	k "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *k.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	w := &k.Writer{
		Addr:     k.TCP(brokers...),
		Topic:    topic,
		Balancer: &k.LeastBytes{},
	}

	return &Producer{w}
}

func (p *Producer) Publish(ctx context.Context, msg k.Message) error {
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
