package inflight

import (
	"sync"
	"math/rand"
)

type QueueDrainer struct {
	QuotaChannel <-chan func()

	QueueByPriority map[PriorityBand]*WRRQueue
}

func (d *QueueDrainer) Run() {
	for {
		select {
		case quotaFinishFunc := <-d.QuotaChannel:
			d.process(quotaFinishFunc)
		}
	}
}

func (d *QueueDrainer) process(quotaFinishFunc func()) {
	for i := 0; i <= int(SystemLowestPriorityBand); i++ {
		if distributionCh := d.QueueByPriority[PriorityBand(i)].dequeue(); distributionCh != nil {
			go func() {
				distributionCh <- struct{}{}
				quotaFinishFunc()
			}()
			return
		}
	}
	quotaFinishFunc()
}

func (d *QueueDrainer) Enqueue(bkt *Bucket) <-chan struct{} {
	// Prioritizing
	distributionCh := make(chan struct{})
	d.QueueByPriority[bkt.Priority].enqueue(bkt.Name, distributionCh)
	return distributionCh
}

func NewWRRQueue(bkts []*Bucket) *WRRQueue {
	q := &WRRQueue{
		lock:    &sync.Mutex{},
		weights: make(map[string]float64),
		queues:  make(map[string][]interface{}),
	}
	for _, bkt := range bkts {
		q.weights[bkt.Name] = float64(bkt.Weight)
	}
	return q
}

type WRRQueue struct {
	lock *sync.Mutex

	weights map[string]float64
	queues  map[string][]interface{}
}

func (q *WRRQueue) enqueue(queueIdentifier string, distributionCh chan<- struct{}) {
	q.lock.Lock()
	defer q.lock.Unlock()
	// Distributing
	q.queues[queueIdentifier] = append(q.queues[queueIdentifier], distributionCh)
}

func (q *WRRQueue) dequeue() (distributionCh chan<- struct{}) {
	// TODO: optimize time cost to Log(N) here by applying segment tree algorithm
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.weights) == 0 {
		return nil
	}

	var totalWeight float64
	for id, w := range q.weights {
		if len(q.queues[id]) == 0 {
			continue
		}
		totalWeight += w
	}

	randomPtr := rand.Float64() * totalWeight
	var distributionPtr float64
	for id, w := range q.weights {
		if len(q.queues[id]) == 0 {
			continue
		}
		distributionPtr += w
		if randomPtr <= distributionPtr {
			distributionCh, q.queues[id] = q.queues[id][0].(chan<- struct{}), q.queues[id][1:]
			return distributionCh
		}
	}

	return nil
}
