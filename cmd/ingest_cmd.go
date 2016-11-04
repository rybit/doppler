package cmd

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

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
	host := config.RedshiftConf.Host
	port := config.RedshiftConf.Port

	wg := new(sync.WaitGroup)
	if config.MetricsConf != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := log.WithField("component", "metrics")
			if err := redshift.ProcessMetrics(host, port, config.MetricsConf, nc, l); err != nil {
				l.WithError(err).Warn("Error while processing metrics")
			} else {
				l.Info("Shutdown processing metrics")
			}
		}()
	}

	//if config.LogLineConf != nil {
	//	logsChan := redshift.ProcessLogs(config, nc, log.WithField("component", "logs"))
	//}

	log.Info("Ingestion started")
	wg.Wait()
	log.Info("Shutting down ingestion")
}

//	// connect to REDSHIFT
//	log.WithFields(logrus.Fields{
//		"db":   config.RedshiftConf.DB,
//		"host": config.RedshiftConf.Host,
//		"port": config.RedshiftConf.Port,
//	}).Info("Connecting to Redshift")
//	db, err := redshift.ConnectToRedshift(&config.RedshiftConf, log)
//
//	err = redshift.CreateTables(db)
//	if err != nil {
//		log.WithError(err).Fatal("Failed to create the necessary tables")
//	}
//
//	redshift.LogQueries = config.RedshiftConf.LogQueries
//
//	writer, wg := getMsgProcessor(config, db, log)
//
//	var sub *nats.Subscription
//	log.WithFields(logrus.Fields{"subject": metricsSubject, "group": metricsGroup}).Info("Subscribing for metrics")
//	if metricsGroup != "" {
//		sub, err = nc.QueueSubscribe(metricsSubject, metricsGroup, writer)
//	} else {
//		sub, err = nc.Subscribe(metricsSubject, writer)
//	}
//	if err != nil {
//		log.WithFields(logrus.Fields{
//			"subject": metricsSubject,
//			"group":   metricsGroup,
//		}).WithError(err).Fatal("Failed to subscribe")
//	}
//
//	defer sub.Unsubscribe()
//	log.Info("Waiting for consumers incoming messages")
//	wg.Wait()
//
//	log.Info("Shutting down ingestion")
//}
//
//// This will start up a pool of consumers that will consume from nats directly.
//// They will each then batch write to redshift
//func getMsgProcessor(config *conf.Config, db *sql.DB, log *logrus.Entry) (nats.MsgHandler, *sync.WaitGroup) {
//	to := time.Duration(config.RedshiftConf.BatchTimeout) * time.Second
//
//	wg := new(sync.WaitGroup)
//	toRedshift := make(chan *nats.Msg, config.NatsConf.BufferSize)
//
//	log.Debugf("Starting %d consumers", config.NatsConf.PoolSize)
//	for i := int64(0); i < config.NatsConf.PoolSize; i++ {
//		l := log.WithField("client", i)
//		l.Infof("Starting consumer %d", i)
//		incoming, off := redshift.StartBatcher(db, to, config.RedshiftConf.BatchSize, l)
//		wg.Add(1)
//
//		go func() {
//			defer func() {
//				wg.Done()
//				off <- true
//			}()
//
//			for m := range toRedshift {
//				payload := new(messaging.InboundMetric)
//				if err := json.Unmarshal(m.Data, payload); err == nil {
//					//go redshift.Insert(db, payload, log)
//					incoming <- payload
//				} else {
//					log.WithError(err).Warn("Failed to parse incoming message")
//				}
//			}
//		}()
//	}
//
//	return func(m *nats.Msg) {
//		toRedshift <- m
//	}, wg
//}
