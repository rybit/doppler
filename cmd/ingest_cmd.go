package cmd

import (
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"

	"time"

	"github.com/rybit/doppler/conf"
	"github.com/rybit/doppler/messaging"
	"github.com/rybit/doppler/redshift"
)

var ingestCmd = &cobra.Command{
	Use: "ingest <subject> [group]",
	Run: ingest,
}

func ingest(cmd *cobra.Command, args []string) {
	config, log := start(cmd)
	if len(args) == 0 {
		log.Fatal("Must provide a subject and optionally a group")
	}

	metricsSubject := args[0]
	metricsGroup := ""
	if len(args) == 2 {
		metricsGroup = args[1]
	}

	log.WithField("version", Version).Info("Configured - starting to connect and consume")

	// connect to REDSHIFT
	log.WithFields(logrus.Fields{
		"db":   config.RedshiftConf.DB,
		"host": config.RedshiftConf.Host,
		"port": config.RedshiftConf.Port,
	}).Info("Connecting to Redshift")
	db, err := redshift.ConnectToRedshift(&config.RedshiftConf, log)

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

	err = redshift.CreateTables(db)
	if err != nil {
		log.WithError(err).Fatal("Failed to create the necessary tables")
	}

	writer, wg := getMsgProcessor(config, db, log)

	var sub *nats.Subscription
	log.WithFields(logrus.Fields{"subject": metricsSubject, "group": metricsGroup}).Info("Subscribing for metrics")
	if metricsGroup != "" {
		sub, err = nc.QueueSubscribe(metricsSubject, metricsGroup, writer)
	} else {
		sub, err = nc.Subscribe(metricsSubject, writer)
	}
	if err != nil {
		log.WithFields(logrus.Fields{
			"subject": metricsSubject,
			"group":   metricsGroup,
		}).WithError(err).Fatal("Failed to subscribe")
	}

	defer sub.Unsubscribe()
	log.Info("Waiting for consumers incoming messages")
	wg.Wait()

	log.Info("Shutting down ingestion")
}

// This will start up a pool of consumers that will consume from nats directly.
// They will each then batch write to redshift
func getMsgProcessor(config *conf.Config, db *sql.DB, log *logrus.Entry) (nats.MsgHandler, *sync.WaitGroup) {
	to := time.Duration(config.RedshiftConf.BatchTimeout) * time.Second

	wg := new(sync.WaitGroup)
	toRedshift := make(chan *nats.Msg, config.NatsConf.BufferSize)

	log.Debugf("Starting %d consumers", config.NatsConf.PoolSize)
	for i := int64(0); i < config.NatsConf.PoolSize; i++ {
		l := log.WithField("client", i)
		l.Infof("Starting consumer %d", i)
		incoming, off := redshift.StartBatcher(db, to, config.RedshiftConf.BatchSize, l)
		wg.Add(1)

		go func() {
			defer func() {
				wg.Done()
				off <- true
			}()

			for m := range toRedshift {
				payload := new(messaging.InboundMetric)
				if err := json.Unmarshal(m.Data, payload); err == nil {
					//go redshift.Insert(db, payload, log)
					incoming <- payload
				} else {
					log.WithError(err).Warn("Failed to parse incoming message")
				}
			}
		}()
	}

	return func(m *nats.Msg) {
		toRedshift <- m
	}, wg
}
