package messaging

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rybit/nats_metrics"
)

func StartReporting(interval int, log *logrus.Entry) {
	if interval <= 0 {
		log.Debug("Skipping stats reporting for consumers - it is configured off")
		return
	}

	log.WithField("interval", interval).Info("Starting to report stats about the consumers")
	go func() {
		flying := metrics.NewGauge("doppler.batches_inflight", nil)
		sent := metrics.NewGauge("doppler.batches_sent", nil)
		for range time.Tick(time.Duration(interval) * time.Second) {
			flying.Set(inflightBatches, nil)
			sent.Set(sentBatches, nil)
		}
	}()
}
