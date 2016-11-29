package messaging

type IngestConfig struct {
	// for writing batches to the service
	BatchTimeout int `mapstructure:"batch_timeout"`
	BatchSize    int `mapstructure:"batch_size"`

	// for consuming off the nats
	Subject    string `mapstructure:"subject"`
	Group      string `mapstructure:"group"`
	PoolSize   int    `mapstructure:"pool_size"`
	BufferSize int    `mapstructure:"buffer_size"`
}
