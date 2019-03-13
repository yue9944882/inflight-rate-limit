package inflight

import (
	"fmt"
	"net/http"
	"time"
)

type InflightRateLimitFilter struct {
	// list-watching API models
	bucketsLister        []*Bucket
	bucketBindingsLister []*BucketBinding

	// running components
	*reservedQuotaManager
	*sharedQuotaManager
	*queueDrainer

	Delegate http.HandlerFunc
}

func (f *InflightRateLimitFilter) Serve(resp http.ResponseWriter, req *http.Request) {

	// 0. Matching request w/ bindings API
	matchedBkt := findMatchedBucket(req, f.bucketsLister, f.bucketBindingsLister)

	// 1. Waiting to be notified by either a reserved quota or a shared quota
	distributionCh := f.queueDrainer.Enqueue(matchedBkt)
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
	quotaCh := make(chan interface{})
	drainer := newQueueDrainer(bkts, quotaCh)
	inflightFilter := &InflightRateLimitFilter{
		bucketsLister:        bkts,
		bucketBindingsLister: bindings,

		queueDrainer: drainer,

		sharedQuotaManager:   newSharedQuotaManager(quotaCh, bkts, drainer),
		reservedQuotaManager: newReservedQuotaManager(quotaCh, bkts, drainer),

		Delegate: func(resp http.ResponseWriter, req *http.Request) {
			time.Sleep(time.Millisecond * 100) // assuming that it takes 100ms to finish the request
			resp.Write([]byte("success!"))
		},
	}

	return inflightFilter
}

func (f *InflightRateLimitFilter) Run() {
	go f.reservedQuotaManager.Run()
	go f.sharedQuotaManager.Run()
	go f.queueDrainer.Run()
}
