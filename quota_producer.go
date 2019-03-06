package inflight

import (
	"sync"
	"time"
)

type quotaProducer struct {
	lock *sync.Mutex

	priority PriorityBand

	remainingSharedQuota int

	drainer *queueDrainer
}

type quotaNotification struct {
	priority   PriorityBand
	finishFunc func()
}

func (c *quotaProducer) Run() {
	for {
		if c.remainingSharedQuota > 0 {
			c.lock.Lock()
			c.remainingSharedQuota--
			c.lock.Unlock()
			c.drainer.Receive(quotaNotification{
				priority: c.priority,
				finishFunc: func() {
					c.lock.Lock()
					defer c.lock.Unlock()
					c.remainingSharedQuota++
				},
			})
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
}
