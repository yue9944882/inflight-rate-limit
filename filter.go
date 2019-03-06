package inflight

import (
	"net/http"
	"time"
	"sync"
	"fmt"
)

const (
	maxTimeout = time.Minute * 10
)

func NewInflightRateLimitFilter(bkts []*Bucket, bindings []*BucketBinding) *InflightRateLimitFilter {
	bkts = append(bkts, ExtraBucket)

	quotaFuncChannel := make(chan func())
	bucketsByName := make(map[string]*Bucket)
	bindingsByPriority := make(map[PriorityBand][]*BucketBinding)
	wrrQueueByPriority := make(map[PriorityBand]*WRRQueue)
	for i := 0; i <= int(SystemLowestPriorityBand); i++ {
		wrrQueueByPriority[PriorityBand(i)] = NewWRRQueue(bkts)
	}

	remainingSharedQuotasByBucket := make(map[string]int)
	remainingReservedQuotasByBucket := make(map[string]int)

	// init
	for _, bkt := range bkts {
		bkt := bkt
		bucketsByName[bkt.Name] = bkt
		remainingReservedQuotasByBucket[bkt.Name] = bkt.ReservedQuota
		remainingSharedQuotasByBucket[bkt.Name] = bkt.SharedQuota
	}
	for _, binding := range bindings {
		binding := binding
		bindingsByPriority[bucketsByName[binding.BucketRef.Name].Priority] = append(bindingsByPriority[bucketsByName[binding.BucketRef.Name].Priority], binding)
	}

	// run
	inflightFilter := &InflightRateLimitFilter{
		BucketsByName:            bucketsByName,
		BucketBindingsByPriority: bindingsByPriority,

		Drainer: &QueueDrainer{
			QuotaChannel:    quotaFuncChannel,
			QueueByPriority: wrrQueueByPriority,
		},
		Collector: &QuotaCollector{
			Lock:                             &sync.Mutex{},
			QuotaFinishFuncChannel:           quotaFuncChannel,
			RemainingSharedQuotasByBucket:    remainingSharedQuotasByBucket,
			RemainingReservedQuotasByBucket:  remainingReservedQuotasByBucket,
			BucketsByName:                    bucketsByName,
			BucketBindingsByPriority:         bindingsByPriority,
		},
		Delegate: func(resp http.ResponseWriter, req *http.Request) {
			resp.Write([]byte("success!"))
		},
	}
	return inflightFilter
}

type InflightRateLimitFilter struct {
	// list-watching API models
	BucketsByName            map[string]*Bucket
	BucketBindingsByPriority map[PriorityBand][]*BucketBinding

	// running components
	Drainer   *QueueDrainer
	Collector *QuotaCollector

	Delegate http.HandlerFunc
}

func (f *InflightRateLimitFilter) Serve(resp http.ResponseWriter, req *http.Request) {

	// 0. Matching request w/ bindings API
	matchedBkt := findMatchedBucket(req, f.BucketsByName, f.BucketBindingsByPriority)

	// 1. Try to acquire reserving quota
	finishFunc := f.Collector.Guarantee(matchedBkt)
	if finishFunc != nil {
		defer finishFunc()
		fmt.Println("guaranteed.")
		f.Delegate(resp, req)
		return
	}

	// 2. Waiting to be distributed
	distributionCh := f.Drainer.Enqueue(matchedBkt)
	ticker := time.NewTicker(maxTimeout)
	defer ticker.Stop()
	select {
	case <-distributionCh:
		// fmt.Println("distributed.")
		f.Delegate(resp, req)
	case <-ticker.C:
		resp.WriteHeader(409)
	}

}
