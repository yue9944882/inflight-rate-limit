package inflight

import (
	"sync"
)

type sharedQuotaNotification struct {
	priority         PriorityBand
	quotaReleaseFunc func()
}

type reservedQuotaNotification struct {
	priority         PriorityBand
	bktName          string
	quotaReleaseFunc func()
}

type quotaProducer struct {
	lock           *sync.Mutex
	bktByName      map[string]*Bucket
	remainingQuota map[string]int
	drainer        *queueDrainer
}

func (c *quotaProducer) Run(quotaProcessFunc func(bkt *Bucket, quotaReleaseFunc func())) {
	for {
		for name, bkt := range c.bktByName {
			func() {
				c.lock.Lock()
				defer c.lock.Unlock()
				if c.remainingQuota[name] > 0 {
					c.remainingQuota[bkt.Name]--
				}
			}()
			quotaProcessFunc(
				bkt,
				func() {
					c.lock.Lock()
					defer c.lock.Unlock()
					c.remainingQuota[bkt.Name]++
				})
		}

	}
}
