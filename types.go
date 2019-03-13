package inflight

type PriorityBand int

const (
	SystemTopPriorityBand    = PriorityBand(iota)
	SystemHighPriorityBand
	SystemMediumPriorityBand
	SystemNormalPorityBand
	SystemLowPriorityBand

	// This is an implicit priority that cannot be set via API
	SystemLowestPriorityBand
)

var ExtraBucket = &Bucket{
	Name:        "__extra",
	SharedQuota: 10,
	Priority:    SystemLowestPriorityBand,
}

type Bucket struct {
	Name string `yaml:"name"`

	ReservedQuota int `yaml:"reservedQuota"`

	SharedQuota int `yaml:"sharedQuota"`

	Weight int `yaml:"weight"`

	Priority PriorityBand `yaml:"priority"`
}

type BucketBinding struct {
	Rules []BucketBindingRule `yaml:"rules"`

	BucketRef BucketReference `yaml:"bucketRef"`
}

type BucketReference struct {
	Name string `yaml:"name"`
}
type BucketBindingRule struct {
	Field  string   `yaml:"field"`
	Values []string `yaml:"values"`
}
