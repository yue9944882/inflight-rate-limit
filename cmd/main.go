package main

import (
	"io/ioutil"
	"net/http"
	"log"

	inflight "github.com/yue9944882/inflight-rate-limit"
	"gopkg.in/yaml.v2"
	"os"
	"path/filepath"
	"flag"
	"fmt"
)

func main() {

	workingDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	exampleDir := flag.String("example-dir", workingDir, "a string")
	flag.Parse()


	fmt.Printf("Reading examples at %v\n", *exampleDir)
	bkts := loadBuckets(*exampleDir)
	bindings := loadBindings(*exampleDir)

	inflightFilter := inflight.NewInflightRateLimitFilter(bkts, bindings)

	inflightFilter.Run()

	http.HandleFunc("/", inflightFilter.Serve)
	fmt.Printf("Serving at 127.0.0.1:8080\n")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func loadBuckets(dir string) []*inflight.Bucket {
	data, err := ioutil.ReadFile(filepath.Join(dir, "buckets.yaml"))
	if err != nil {
		panic(err)
	}
	var buckets []*inflight.Bucket
	if err := yaml.Unmarshal(data, &buckets); err != nil {
		panic(err)
	}
	return buckets
}

func loadBindings(dir string) []*inflight.BucketBinding {
	data, err := ioutil.ReadFile(filepath.Join(dir, "bindings.yaml"))
	if err != nil {
		panic(err)
	}
	var bindings []*inflight.BucketBinding
	if err := yaml.Unmarshal(data, &bindings); err != nil {
		panic(err)
	}
	return bindings
}
