package scalyr

const (
	scalyrIngestURL = "https://www.scalyr.com/addEvents"
)

type ScalyrConfig struct {
	BatchTimeout int `mapstructure:"batch_timeout"`
	BatchSize    int `mapstructure:"batch_size"`

	Subject    string `mapstructure:"subject"`
	Group      string `mapstructure:"group"`
	PoolSize   int    `mapstructure:"pool_size"`
	BufferSize int    `mapstructure:"buffer_size"`
}
