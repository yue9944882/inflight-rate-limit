package inflight

import "testing"

func TestQueueDrainerProcessExtraBucket(t *testing.T) {
	quotaCh := make(chan func())
	queueByPriority := make(map[PriorityBand]*WRRQueue)
	for i := 0; i < int(SystemLowestPriorityBand); i++ {
		queueByPriority[PriorityBand(i)] = NewWRRQueue(nil)
	}

	drainer := &QueueDrainer{
		QuotaChannel:    quotaCh,
		QueueByPriority: queueByPriority,
	}
	finished := false
	drainer.process(func() {
		finished = true
	})
	if !finished {
		t.Fatal("finish func not executed")
	}
}
