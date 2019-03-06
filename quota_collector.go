package inflight

import (
	"sync"
)

type QuotaCollector struct {
	Lock *sync.Mutex

	QuotaFinishFuncChannel chan<- func()

	RemainingSharedQuotasByBucket map[string]int

	RemainingReservedQuotasByBucket map[string]int

	BucketsByName            map[string]*Bucket
	BucketBindingsByPriority map[PriorityBand][]*BucketBinding
}

func (c *QuotaCollector) Run() {
	for {
		for _, bkt := range c.BucketsByName {
			// distribute shared quotas
			c.Lock.Lock()
			remainingSharedQuota := c.RemainingSharedQuotasByBucket[bkt.Name]
			c.Lock.Unlock()

			for i := 0; i < remainingSharedQuota; i++ {
				c.Lock.Lock()
				c.RemainingSharedQuotasByBucket[bkt.Name]--
				c.Lock.Unlock()

				c.QuotaFinishFuncChannel <- func() {
					c.Lock.Lock()
					defer c.Lock.Unlock()
					c.RemainingSharedQuotasByBucket[bkt.Name]++
				}
			}
		}
	}
}

func (c *QuotaCollector) Guarantee(bkt *Bucket) (finishFunc func()) {
	// Reserving
	c.Lock.Lock()
	defer c.Lock.Unlock()
	if c.RemainingReservedQuotasByBucket[bkt.Name] > 0 {
		c.RemainingReservedQuotasByBucket[bkt.Name]--
		return func() {
			c.Lock.Lock()
			defer c.Lock.Unlock()
			c.RemainingReservedQuotasByBucket[bkt.Name]++
		}
	}
	return nil
}
