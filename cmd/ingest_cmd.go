package cmd

import (
	"database/sql"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/nats-io/nats"
	"github.com/rybit/doppler/conf"
	"github.com/rybit/doppler/messaging"
	"github.com/rybit/doppler/redshift"
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
			err := process(
				config.MetricsConf,
				nc,
				l,
				redshift.CreateMetricsTables,
				redshift.BuildMetricsHandler,
			)
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
			err := process(
				config.LogLineConf,
				nc,
				l,
				redshift.CreateLogsTables,
				redshift.BuildLogsHandler,
			)
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

type tableCreator func(*sql.DB) error
type handlerBuilder func(*sql.DB, bool) messaging.BatchHandler

func process(config *conf.IngestionConfig, nc *nats.Conn, log *logrus.Entry, tc tableCreator, hb handlerBuilder) error {
	log.WithFields(logrus.Fields{
		"db":   config.DB,
		"host": config.Host,
		"port": config.Port,
	}).Info("Connecting to Redshift")
	db, err := redshift.ConnectToRedshift(
		config.Host,
		config.Port,
		config.DB,
		config.User,
		config.Pass,
		config.Timeout,
	)
	if err != nil {
		return err
	}

	if err := tc(db); err != nil {
		return err
	}

	// build worker pool
	shared := make(chan *nats.Msg, config.BufferSize)
	bufSub := &messaging.BufferedSubscriber{
		Subject:  config.Subject,
		Group:    config.Group,
		Messages: shared,
	}
	wg, err := messaging.BuildBatchingWorkerPool(shared, config.PoolSize, config.BatchSize, config.BatchTimeout, log, hb(db, config.LogQueries))
	if err != nil {
		return err
	}

	if err := bufSub.Subscribe(nc, log); err != nil {
		return err
	}
	defer bufSub.Unsubscribe()

	wg.Wait()
	return nil
}
