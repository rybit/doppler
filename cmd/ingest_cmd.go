package cmd

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

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
	if config.MetricsConf != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := log.WithField("component", "metrics")
			err := redshift.ProcessMetrics(nc, l, config.MetricsConf)
			if err != nil {
				l.WithError(err).Warn("Error while processing metrics")
			} else {
				l.Info("Shutdown processing metrics")
			}
		}()
	}

	if config.LogLineConf != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := log.WithField("component", "logs")
			err := redshift.ProcessLines(nc, l, config.LogLineConf)
			if err != nil {
				l.WithError(err).Warn("Error while processing logs")
			} else {
				l.Info("Shutdown processing logs")
			}
		}()
	}

	if config.ScalyrConf != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := log.WithField("component", "scalyr")
			err := scalyr.ProcessLogsToScalyr(nc, l, config.ScalyrConf)
			if err != nil {
				l.WithError(err).Warn("Error while processing logs")
			} else {
				l.Info("Shutdown processing logs")
			}
		}()
	}

	log.Info("Ingestion started")
	wg.Wait()
	log.Info("Shutting down ingestion")
}
