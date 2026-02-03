package kafka

import "github.com/segmentio/kafka-go"

func NewConsumer(brokers []string, groupId string, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupId,
		Topic:    topic,
		MinBytes: 1e3,
		MaxBytes: 10e6,
	})
}
