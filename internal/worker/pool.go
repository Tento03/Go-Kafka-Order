package worker

import (
	"context"
	"go-kafka-order/internal/model"
	"log"
	"sync"
)

type Job struct {
	order model.Order
	ack   func(success bool)
}

type Pool struct {
	jobs chan Job
	wg   sync.WaitGroup
}

func NewPool(size int) *Pool {
	return &Pool{jobs: make(chan Job)}
}

func (p *Pool) Run(ctx context.Context, size int) {
	for i := 0; i < size; i++ {
		p.wg.Add(1)

		go func(workerId int) {
			defer p.wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case job := <-p.jobs:
					log.Printf("Worker %d processing order %s", workerId, job.order.ID)

					if job.order.Quantity%2 == 0 {
						job.ack(true)
					} else {
						job.ack(false)
					}
				}
			}
		}(i)
	}
}

func (p *Pool) Submit(job Job) {
	p.jobs <- job
}

func (p *Pool) Stop() {
	close(p.jobs)
	p.wg.Wait()
}
