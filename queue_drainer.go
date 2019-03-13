package inflight

import (
	"sync"
	"math/rand"
	"time"
)

func newQueueDrainer(bkts []*Bucket, quotaCh <-chan interface{}) *queueDrainer {
	wrrQueueByPriority := make(map[PriorityBand]*WRRQueue)

	totalSharedQuota := 0
	sharedQuotaByPriority := make(map[PriorityBand]int)
	for _, bkt := range bkts {
		bkt := bkt
		sharedQuotaByPriority[bkt.Priority] += bkt.SharedQuota
		totalSharedQuota += bkt.SharedQuota
	}

	for i := 0; i <= int(SystemLowestPriorityBand); i++ {
		wrrQueueByPriority[PriorityBand(i)] = NewWRRQueue(bkts)
	}

	drainer := &queueDrainer{
		lock:              &sync.Mutex{},
		maxQueueLength:    totalSharedQuota,
		queueByPriorities: wrrQueueByPriority,
		quotaCh:           quotaCh,
	}
	return drainer
}

type queueDrainer struct {
	lock           *sync.Mutex
	queueLength    int
	maxQueueLength int

	quotaCh <-chan interface{}

	queueByPriorities map[PriorityBand]*WRRQueue
}

func (d *queueDrainer) Run() {
	for {
		if d.queueLength == 0 {
			// HACK: avoid racing empty queue
			time.Sleep(time.Millisecond * 20)
		}
		quota := <-d.quotaCh
		switch quota := quota.(type) {
		case reservedQuotaNotification:
			func() {
				d.lock.Lock()
				defer d.lock.Unlock()
				distributionCh := d.PopByPriorityAndBucketName(quota.priority, quota.bktName)
				if distributionCh != nil {
					go func() {
						distributionCh <- quota.quotaReleaseFunc
					}()
					d.queueLength--
					return
				} else {
					quota.quotaReleaseFunc()
				}
			}()
		case sharedQuotaNotification:
			func() {
				d.lock.Lock()
				defer d.lock.Unlock()
				for j := int(SystemTopPriorityBand); j <= int(quota.priority); j++ {
					distributionCh := d.PopByPriority(quota.priority)
					if distributionCh != nil {
						go func() {
							distributionCh <- quota.quotaReleaseFunc
						}()
						d.queueLength--
						return
					}
				}
				quota.quotaReleaseFunc()
			}()
		}
	}
}

func (d *queueDrainer) PopByPriority(band PriorityBand) (releaseQuotaFuncCh chan<- func()) {
	return d.queueByPriorities[band].dequeue()
}

func (d *queueDrainer) PopByPriorityAndBucketName(band PriorityBand, bktName string) (releaseQuotaFuncCh chan<- func()) {
	return d.queueByPriorities[band].dequeueDirectly(bktName)
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

func (q *WRRQueue) enqueue(key string, distributionCh chan<- func()) {
	q.lock.Lock()
	defer q.lock.Unlock()
	// Distributing
	q.queues[key] = append(q.queues[key], distributionCh)
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

func (q *WRRQueue) dequeueDirectly(key string) (distributeCh chan<- func()) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.queues[key]) == 0 {
		return nil
	}
	distributeCh, q.queues[key] = q.queues[key][0].(chan<- func()), q.queues[key][1:]
	return
}
