package inflight

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type InflightRateLimitFilter struct {
	// list-watching API models
	buckets                    map[string]*Bucket
	bucketBindingsByPriorities map[PriorityBand][]*BucketBinding

	// running components
	*reservedQuotaManager
	*sharedQuotaManager

	Delegate http.HandlerFunc
}

func (f *InflightRateLimitFilter) Serve(resp http.ResponseWriter, req *http.Request) {

	// 0. Matching request w/ bindings API
	matchedBkt := findMatchedBucket(req, f.buckets, f.bucketBindingsByPriorities)

	// 1. Try to acquire reserving quota
	finishFunc := f.reservedQuotaManager.Guarantee(matchedBkt)
	if finishFunc != nil {
		defer finishFunc()
		fmt.Println("guaranteed.")
		f.Delegate(resp, req)
		return
	}

	// 2. Waiting to be distributed
	distributionCh := f.drainer.Enqueue(matchedBkt)
	if distributionCh == nil {
		// too many requests
		resp.WriteHeader(409)
	}
	ticker := time.NewTicker(maxTimeout)
	defer ticker.Stop()
	select {
	case finishFunc := <-distributionCh:
		fmt.Println("distributed.")
		defer finishFunc()
		f.Delegate(resp, req)
	case <-ticker.C:
		resp.WriteHeader(409)
	}

}

const (
	maxTimeout = time.Minute * 10
)

func NewInflightRateLimitFilter(bkts []*Bucket, bindings []*BucketBinding) *InflightRateLimitFilter {

	// 1. We add an logical extra bucket which holds the global extra shared quota to the list
	bkts = append(bkts, ExtraBucket)

	for _, bkt := range bkts {
		fmt.Printf("*Bucket* %v: [RESERVED] %v, [SHARED] %v\n", bkt.Name, bkt.ReservedQuota, bkt.SharedQuota)
	}

	// 2. Initializing everything
	bucketsByName := make(map[string]*Bucket)
	bindingsByPriority := make(map[PriorityBand][]*BucketBinding)
	wrrQueueByPriority := make(map[PriorityBand]*WRRQueue)

	totalSharedQuota := 0
	sharedQuotaByPriority := make(map[PriorityBand]int)
	for _, bkt := range bkts {
		bkt := bkt
		sharedQuotaByPriority[bkt.Priority] += bkt.SharedQuota
		totalSharedQuota += bkt.SharedQuota
	}

	remainingSharedQuotasByBucket := make(map[string]int)
	remainingReservedQuotasByBucket := make(map[string]int)

	for _, bkt := range bkts {
		bkt := bkt
		bucketsByName[bkt.Name] = bkt
		remainingReservedQuotasByBucket[bkt.Name] = bkt.ReservedQuota
		remainingSharedQuotasByBucket[bkt.Name] = bkt.SharedQuota
	}

	fmt.Printf("*Queue drainer*: Distributing %v shared quotas\n", totalSharedQuota)

	for _, binding := range bindings {
		binding := binding
		bindingsByPriority[bucketsByName[binding.BucketRef.Name].Priority] = append(bindingsByPriority[bucketsByName[binding.BucketRef.Name].Priority], binding)
	}

	drainer := &queueDrainer{
		lock:                         &sync.Mutex{},
		maxQueueLength:               totalSharedQuota,
		queueByPriorities:            wrrQueueByPriority,
		quotaReleasingFuncByPriority: make(map[PriorityBand][]func()),
	}

	var quotaProducers []*quotaProducer
	for i := 0; i <= int(SystemLowestPriorityBand); i++ {
		wrrQueueByPriority[PriorityBand(i)] = NewWRRQueue(bkts)
		quotaProducers = append(quotaProducers, &quotaProducer{
			lock:                 &sync.Mutex{},
			priority:             PriorityBand(i),
			remainingSharedQuota: sharedQuotaByPriority[PriorityBand(i)],
			drainer:              drainer,
		})
	}

	// run
	inflightFilter := &InflightRateLimitFilter{
		buckets:                    bucketsByName,
		bucketBindingsByPriorities: bindingsByPriority,


		sharedQuotaManager: &sharedQuotaManager{
			producers: quotaProducers,
			drainer:   drainer,
		},
		reservedQuotaManager: &reservedQuotaManager{
			lock:                             &sync.Mutex{},
			remainingReservedQuotasByBuckets: remainingReservedQuotasByBucket,
		},

		Delegate: func(resp http.ResponseWriter, req *http.Request) {
			time.Sleep(time.Millisecond * 100) // assuming that it takes 100ms to finish the request
			resp.Write([]byte("success!"))
		},
	}

	return inflightFilter
}

func (f *InflightRateLimitFilter) Run() {
	go f.drainer.Run()
	for _, producer := range f.producers {
		producer := producer
		go producer.Run()
	}
}
