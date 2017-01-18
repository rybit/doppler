package kairos

import "github.com/rybit/doppler/messaging"

type Config struct {
	HTTPConfig  `mapstructure:",squash"`
	MetricsConf *messaging.IngestConfig `mapstructure:"metrics_conf"`
}
