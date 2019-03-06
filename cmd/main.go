package main

import (
	inflight "github.com/yue9944882/inflight-rate-limit"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/http"
	"log"
)

func main() {

	bkts := loadBuckets()
	bindings := loadBindings()

	inflightFilter := inflight.NewInflightRateLimitFilter(bkts, bindings)

	go inflightFilter.Drainer.Run()
	go inflightFilter.Collector.Run()

	http.HandleFunc("/", inflightFilter.Serve)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func loadBuckets() []*inflight.Bucket {
	data, err := ioutil.ReadFile("buckets.yaml")
	if err != nil {
		panic(err)
	}
	var buckets []*inflight.Bucket
	if err := yaml.Unmarshal(data, &buckets); err != nil {
		panic(err)
	}
	return buckets
}

func loadBindings() []*inflight.BucketBinding {
	data, err := ioutil.ReadFile("bindings.yaml")
	if err != nil {
		panic(err)
	}
	var bindings []*inflight.BucketBinding
	if err := yaml.Unmarshal(data, &bindings); err != nil {
		panic(err)
	}
	return bindings
}
