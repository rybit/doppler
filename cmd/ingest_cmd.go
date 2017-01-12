package cmd

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

	"os"

	"github.com/nats-io/nats"
	"github.com/rybit/doppler/influx"
	"github.com/rybit/doppler/messaging"
	"github.com/rybit/doppler/redshift"
	"github.com/rybit/doppler/scalyr"
	"github.com/rybit/nats_logrus_hook"
	"github.com/rybit/nats_metrics"
)

var ingestCmd = &cobra.Command{
	Use: "ingest",
	Run: ingest,
}

func ingest(cmd *cobra.Command, _ []string) {
	config, log := start(cmd)
	log = log.WithField("version", Version)
	log.Info("Configured - starting to connect and consume")

	fields := logrus.Fields{
		"servers": config.NatsConf.Servers,
	}
	if config.NatsConf.TLSConfig != nil {
		fields["ca_files"] = config.NatsConf.TLSConfig.CAFiles
		fields["key_file"] = config.NatsConf.TLSConfig.KeyFile
		fields["cert_file"] = config.NatsConf.TLSConfig.CertFile
	}
	// connect to NATS
	log.WithFields(fields).Info("Connecting to Nats")
	nc, err := messaging.ConnectToNats(&config.NatsConf, messaging.ErrorHandler(log))
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to nats")
	}

	if config.NatsConf.LogsSubject != "" {
		logrus.AddHook(nhook.NewNatsHook(nc, config.NatsConf.LogsSubject))
	}

	if config.NatsConf.MetricsSubject != "" {
		metrics.Init(nc, config.NatsConf.MetricsSubject)
		h, err := os.Hostname()
		if err != nil {
			log.WithError(err).Fatal("Failed to get host for metrics dimension")
		}
		metrics.AddDimension("hostname", h)
	} else {
		metrics.Init(nil, "")
	}
	messaging.StartReporting(config.ReportSec, log.WithField("reporting", true))
	wg := new(sync.WaitGroup)
	startRedshiftConsumers(wg, nc, log, config.RedshiftConf)
	startScalyrConsumers(wg, nc, log, config.ScalyrConf)
	startInfluxConsumers(wg, nc, log, config.InfluxConf)

	log.Info("Consumers started")
	wg.Wait()
	log.Info("All consumers stopped - shutting down")
}


func startRedshiftConsumers(wg *sync.WaitGroup, nc *nats.Conn, log *logrus.Entry, config *redshift.Config) {
	if config == nil {
		return
	}

	log = log.WithField("destination", "redshift")

	if config.MetricsConf != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := log.WithField("origin", "metrics")
			err := redshift.ProcessMetrics(nc, l, config)
			if err != nil {
				l.WithError(err).Warn("Error while processing metrics")
			} else {
				l.Info("Shutdown processing metrics")
			}
		}()
	}

	if config.LinesConf != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := log.WithField("origin", "lines")
			err := redshift.ProcessLines(nc, l, config)
			if err != nil {
				l.WithError(err).Warn("Error while processing lines")
			} else {
				l.Info("Shutdown processing lines")
			}
		}()
	}
}

func startScalyrConsumers(wg *sync.WaitGroup, nc *nats.Conn, log *logrus.Entry, config *scalyr.Config) {
	if config == nil {
		return
	}

	log = log.WithField("destination", "scalyr")
	wg.Add(1)
	go func() {
		defer wg.Done()
		l := log.WithField("origin", "logs")
		err := scalyr.ProcessLines(nc, l, config)
		if err != nil {
			l.WithError(err).Warn("Error while processing logs")
		} else {
			l.Info("Shutdown processing logs")
		}
	}()
}

func startInfluxConsumers(wg *sync.WaitGroup, nc *nats.Conn, log *logrus.Entry, config *influx.Config) {
	if config == nil && config.MetricsConf != nil {
		return
	}

	log = log.WithField("destination", "influx")
	wg.Add(1)
	go func() {
		defer wg.Done()
		l := log.WithField("origin", "metrics")
		err := influx.ProcessMetrics(nc, l, config)
		if err != nil {
			l.WithError(err).Warn("Error while processing logs")
		} else {
			l.Info("Shutdown processing logs")
		}
	}()
}
