package inflight

import "net/http"

type subject struct {
	username  string
	namespace string
	verb      string
}

func getSubject(req *http.Request) *subject {
	return &subject{
		username:  req.Header.Get("USER"),
		namespace: req.Header.Get("NAMESPACE"),
		verb:      req.Method,
	}
}

func findMatchedBucket(req *http.Request, bkts []*Bucket, bindings []*BucketBinding) *Bucket {
	sub := getSubject(req)
	// TODO: optimize time cost here
	for _, binding := range bindings {
		if match(sub, binding) {
			for _, bkt := range bkts {
				if bkt.Name == binding.BucketRef.Name {
					return bkt
				}
			}
		}
	}
	return ExtraBucket
}

func match(sub *subject, binding *BucketBinding) bool {
	for _, rule := range binding.Rules {
		switch rule.Field {
		case "user.name":
			for _, v := range rule.Values {
				if v == sub.username {
					return true
				}
			}
		case "namespace":
			for _, v := range rule.Values {
				if v == sub.namespace {
					return true
				}
			}
		case "verb":
			for _, v := range rule.Values {
				if v == sub.verb {
					return true
				}
			}
		}
	}
	return false
}
