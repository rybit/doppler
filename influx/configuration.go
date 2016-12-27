package influx

import (
	"github.com/rybit/doppler/messaging"
	"github.com/rybit/doppler/tls"
)

type Config struct {
	Addr        string                  `mapstructure:"addr"`
	DB          string                  `mapstructure:"db"`
	User        string                  `masptructure:"user"`
	Pass        string                  `mapstructure:"pass"`
	MetricsConf *messaging.IngestConfig `mapstructure:"metrics_conf"`
	TLS         *tls.Config             `mapstructure:"tls"`
}
