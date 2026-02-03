package config

var (
	Brokers = []string{"localhost:9092"}

	OrdersTopic = "orders"
	RetryTopic  = "orders.retry"
	DLQTopic    = "orders.dlq"
)
