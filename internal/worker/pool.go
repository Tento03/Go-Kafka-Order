package worker

import (
	"context"
	"go-kafka-order/internal/model"
	"log"
	"sync"
	"time"
)

type Job struct {
	Order model.Order
	Ack   func(success bool)
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
				case job, ok := <-p.jobs:
					if !ok {
						return
					}

					log.Printf("Worker %d processing order %s", workerId, job.Order.ID)

					jobCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
					err := ProcessOrder(jobCtx, job.Order)
					cancel()

					if err != nil {
						log.Printf("Worker %d ,failed process order %s:%v", workerId, job.Order.ID, err)
						job.Ack(false)
						continue
					}

					log.Printf("Worker %d completed order %s", workerId, job.Order.ID)
					job.Ack(true)
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
