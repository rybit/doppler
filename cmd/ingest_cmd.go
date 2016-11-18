package cmd

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/nats-io/nats"
	"github.com/rybit/doppler/influx"
	"github.com/rybit/doppler/messaging"
	"github.com/rybit/doppler/redshift"
	"github.com/rybit/doppler/scalyr"
)

var ingestCmd = &cobra.Command{
	Use: "ingest",
	Run: ingest,
}

func ingest(cmd *cobra.Command, args []string) {
	config, log := start(cmd)
	log = log.WithField("version", Version)
	log.Info("Configured - starting to connect and consume")

	// connect to NATS
	log.WithFields(logrus.Fields{
		"servers":   config.NatsConf.Servers,
		"ca_files":  config.NatsConf.CAFiles,
		"key_file":  config.NatsConf.KeyFile,
		"cert_file": config.NatsConf.CertFile,
	}).Info("Connecting to Nats")
	nc, err := messaging.ConnectToNats(&config.NatsConf, messaging.ErrorHandler(log))
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to nats")
	}

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
