package inflight

import "sync"

type reservedQuotaManager struct {
	lock                             *sync.Mutex
	remainingReservedQuotasByBuckets map[string]int
}

func (c *reservedQuotaManager) Guarantee(bkt *Bucket) (finishFunc func()) {
	// Reserving
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.remainingReservedQuotasByBuckets[bkt.Name] > 0 {
		c.remainingReservedQuotasByBuckets[bkt.Name]--
		return func() {
			c.lock.Lock()
			defer c.lock.Unlock()
			c.remainingReservedQuotasByBuckets[bkt.Name]++
		}
	}
	return nil
}

type sharedQuotaManager struct {
	producers []*quotaProducer
	drainer   *queueDrainer
}
