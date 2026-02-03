package kafka

import (
	"strconv"

	"github.com/segmentio/kafka-go"
)

const RetryHeader = "retry-count"

func GetRetryCount(headers []kafka.Header) int {
	for _, h := range headers {
		if h.Key == RetryHeader {
			v, _ := strconv.Atoi(string(h.Value))
			return v
		}
	}
	return 0
}

func SetRetryCount(count int) []kafka.Header {
	return []kafka.Header{
		{Key: RetryHeader, Value: []byte(strconv.Itoa(count))},
	}
}
