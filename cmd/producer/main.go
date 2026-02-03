package producer

import (
	"context"
	"encoding/json"
	"go-kafka-order/internal/config"
	"go-kafka-order/internal/kafka"
	"go-kafka-order/internal/model"
	"log"
	"net/http"
	"time"

	k "github.com/segmentio/kafka-go"

	"github.com/google/uuid"
)

func main() {
	producer := kafka.NewProducer(config.Brokers, config.OrdersTopic)
	defer producer.Close()

	http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		var o model.Order
		json.NewDecoder(r.Body).Decode(&o)

		o.ID = uuid.NewString()
		o.CreatedAt = time.Now()

		log.Printf("Order received: %s", o.ID)

		b, _ := json.Marshal(o)
		msg := k.Message{Value: b}

		producer.Publish(context.Background(), msg)
		w.WriteHeader(http.StatusAccepted)
	})

	log.Println("Producer running on :8080")
	http.ListenAndServe(":8080", nil)
}
