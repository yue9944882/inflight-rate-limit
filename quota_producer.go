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
}

func (c *quotaProducer) Run(quotaProcessFunc func(bkt *Bucket, quotaReleaseFunc func())) {
	for {
		for name, bkt := range c.bktByName {
			bkt := bkt
			gotQuota := false
			func() {
				c.lock.Lock()
				defer c.lock.Unlock()
				if c.remainingQuota[name] > 0 {
					c.remainingQuota[bkt.Name]--
					gotQuota = true
				}
			}()
			if gotQuota {
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
}
