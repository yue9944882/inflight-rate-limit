package inflight

import (
	"sync"
	"math/rand"
)

type queueDrainer struct {
	lock                         *sync.Mutex
	queueLength                  int
	maxQueueLength               int
	quotaReleasingFuncByPriority map[PriorityBand][]func()
	queueByPriorities            map[PriorityBand]*WRRQueue
}

func (d *queueDrainer) Run() {
	for {
		for i := int(SystemTopPriorityBand); i <= int(SystemLowestPriorityBand); i++ {
			func() {
				d.lock.Lock()
				defer d.lock.Unlock()

				length := len(d.quotaReleasingFuncByPriority[PriorityBand(i)])
				if length == 0 {
					return
				}
				finishFunc := d.quotaReleasingFuncByPriority[PriorityBand(i)][0]

				for j := int(SystemTopPriorityBand); j <= i; j++ {
					distributionCh := d.Pop(PriorityBand(j))
					if distributionCh != nil {
						go func() {
							distributionCh <- finishFunc
						}()
						d.queueLength--
						return
					}
				}
				// re-claim the quota
				d.quotaReleasingFuncByPriority[PriorityBand(i)] = append(d.quotaReleasingFuncByPriority[PriorityBand(i)], finishFunc)
			}()
		}
	}
}

func (d *queueDrainer) Receive(notification quotaNotification) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.quotaReleasingFuncByPriority[notification.priority] = append(
		d.quotaReleasingFuncByPriority[notification.priority], notification.finishFunc)
}

func (d *queueDrainer) Pop(band PriorityBand) chan<- func() {
	return d.queueByPriorities[band].dequeue()
}

func (d *queueDrainer) Enqueue(bkt *Bucket) <-chan func() {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.queueLength > d.maxQueueLength {
		return nil
	}
	// Prioritizing
	d.queueLength++
	distributionCh := make(chan func(), 1)
	d.queueByPriorities[bkt.Priority].enqueue(bkt.Name, distributionCh)
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

func (q *WRRQueue) enqueue(queueIdentifier string, distributionCh chan<- func()) {
	q.lock.Lock()
	defer q.lock.Unlock()
	// Distributing
	q.queues[queueIdentifier] = append(q.queues[queueIdentifier], distributionCh)
}

func (q *WRRQueue) dequeue() (distributionCh chan<- func()) {
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
			distributionCh, q.queues[id] = q.queues[id][0].(chan<- func()), q.queues[id][1:]
			return distributionCh
		}
	}

	return nil
}
