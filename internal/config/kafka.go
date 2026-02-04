package config

var (
	Brokers = []string{"localhost:9092"}
	GroupId = "order-processors"

	OrdersTopic = "orders"
	RetryTopic  = "orders.retry"
	DLQTopic    = "orders.dlq"
)
