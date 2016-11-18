package influx

import (
	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/rybit/doppler/messaging"
)

type Config struct {
	MetricsConf *messaging.IngestConfig `mapstructure:"metrics_conf"`
}

func ProcessMetrics(nc *nats.Conn, log *logrus.Entry, config *Config) error {

	return nil
}
