package worker

import (
	"context"
	"errors"
	"go-kafka-order/internal/model"
	"math/rand"
	"time"
)

func ProcessOrder(ctx context.Context, o model.Order) error {
	if o.Quantity <= 0 {
		return errors.New("invalid quantity")
	}

	if o.Quantity > 100 {
		return errors.New("stock not sufficient")
	}

	if o.ProductName == "" {
		return errors.New("product name is empty")
	}

	select {
	case <-time.After(300 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	if rand.Intn(100) < 30 {
		return errors.New("inventory service timeout")
	}

	return nil
}
