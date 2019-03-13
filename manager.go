package inflight

import (
	"sync"
)

type reservedQuotaManager struct {
	producer *quotaProducer
	drainer  *queueDrainer
	quotaCh  chan<- interface{}
}

func newReservedQuotaManager(quotaCh chan<- interface{}, bkts []*Bucket, drainer *queueDrainer) *reservedQuotaManager {
	mgr := &reservedQuotaManager{
		drainer: drainer,
		producer: &quotaProducer{
			lock:           &sync.Mutex{},
			remainingQuota: make(map[string]int),
			bktByName:      make(map[string]*Bucket),
			drainer:        drainer,
		},
		quotaCh: quotaCh,
	}
	for _, bkt := range bkts {
		bkt := bkt
		mgr.producer.remainingQuota[bkt.Name] += bkt.ReservedQuota
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

func newSharedQuotaManager(quotaCh chan<- interface{}, bkts []*Bucket, drainer *queueDrainer) *sharedQuotaManager {
	mgr := &sharedQuotaManager{
		drainer:   drainer,
		producers: make(map[PriorityBand]*quotaProducer),
		quotaCh:   quotaCh,
	}
	for i := 0; i <= int(SystemLowestPriorityBand); i++ {
		mgr.producers[PriorityBand(i)] = &quotaProducer{
			lock:           &sync.Mutex{},
			drainer:        drainer,
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
	drainer   *queueDrainer
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
