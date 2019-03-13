package inflight

import (
	"sync"
)

type reservedQuotaManager struct {
	producer *quotaProducer
	quotaCh  chan<- interface{}
}

func newReservedQuotaManager(quotaCh chan<- interface{}, bkts []*Bucket) *reservedQuotaManager {
	mgr := &reservedQuotaManager{
		producer: &quotaProducer{
			lock:           &sync.Mutex{},
			remainingQuota: make(map[string]int),
			bktByName:      make(map[string]*Bucket),
		},
		quotaCh: quotaCh,
	}
	for _, bkt := range bkts {
		bkt := bkt
		mgr.producer.remainingQuota[bkt.Name] += bkt.ReservedQuota
		mgr.producer.bktByName[bkt.Name] = bkt
	}
	return mgr
}

func (m *reservedQuotaManager) Run() {
	go m.producer.Run(func(bkt *Bucket, quotaReleaseFunc func()) {
		m.quotaCh <- reservedQuotaNotification{
			priority:         bkt.Priority,
			bktName:          bkt.Name,
			quotaReleaseFunc: quotaReleaseFunc,
		}
	})
}

func newSharedQuotaManager(quotaCh chan<- interface{}, bkts []*Bucket) *sharedQuotaManager {
	mgr := &sharedQuotaManager{
		producers: make(map[PriorityBand]*quotaProducer),
		quotaCh:   quotaCh,
	}
	for i := 0; i <= int(SystemLowestPriorityBand); i++ {
		mgr.producers[PriorityBand(i)] = &quotaProducer{
			lock:           &sync.Mutex{},
			remainingQuota: make(map[string]int),
			bktByName:      make(map[string]*Bucket),
		}
	}
	for _, bkt := range bkts {
		bkt := bkt
		mgr.producers[bkt.Priority].remainingQuota[bkt.Name] += bkt.SharedQuota
		mgr.producers[bkt.Priority].bktByName[bkt.Name] = bkt
	}
	return mgr
}

type sharedQuotaManager struct {
	producers map[PriorityBand]*quotaProducer
	quotaCh   chan<- interface{}
}

func (m *sharedQuotaManager) Run() {
	for _, producer := range m.producers {
		producer := producer
		go producer.Run(func(bkt *Bucket, quotaReleaseFunc func()) {
			m.quotaCh <- sharedQuotaNotification{
				priority:         bkt.Priority,
				quotaReleaseFunc: quotaReleaseFunc,
			}
		})
	}
}
