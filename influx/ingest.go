package influx

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/nats-io/nats"
	"github.com/rybit/nats_metrics"

	"crypto/tls"

	"strings"

	"github.com/rybit/doppler/messaging"
)

func ProcessMetrics(nc *nats.Conn, log *logrus.Entry, config *Config) error {
	if config.MetricsConf == nil {
		return errors.New("Must provide a metrics configuration")
	}

	var tls *tls.Config
	var err error
	if config.TLS != nil {
		log.WithFields(logrus.Fields{
			"ca_files":  strings.Join(config.TLS.CAFiles, ","),
			"key_file":  config.TLS.KeyFile,
			"cert_file": config.TLS.CertFile,
		}).Info("Creating TLS context")
		tls, err = config.TLS.TLSConfig()
		if err != nil {
			log.WithError(err).Warn("Failed to create tls config")
			return err
		}
	}

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:      config.Addr,
		TLSConfig: tls,
		Username:  config.User,
		Password:  config.Pass,
		UserAgent: "doppler",
	})
	if err != nil {
		log.WithError(err).Warn("Failed to create HTTP client")
		return err
	}

	handler := buildHandler(c, config.DB)
	sub, wg, err := messaging.ConsumeInBatches(
		nc,
		log.WithField("db", config.DB),
		config.MetricsConf,
		handler,
	)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	wg.Wait()
	return nil
}

func buildHandler(c client.Client, db string) messaging.BatchHandler {
	return func(batch map[time.Time]*nats.Msg, log *logrus.Entry) {
		start := time.Now()
		timer := metrics.NewTimer("doppler.influx.batch_dur", nil)
		log = log.WithFields(logrus.Fields{
			"batch_id": fmt.Sprintf("%d", start.Nanosecond()),
		})

		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  db,
			Precision: "ns",
		})
		if err != nil {
			log.WithError(err).Warn("Failed to create batch for points")
			return
		}

		// now we will walk the batch and see whats up
		parseFailed := 0
		parseSuccess := 0
		log.Debug("starting to handle batch")
		parsingTimer := metrics.NewTimer("doppler.influx.batch_parsing_dur", nil)
		for _, raw := range batch {
			l := log.WithField("subject", raw.Subject)
			m := new(metrics.RawMetric)
			if err := json.Unmarshal(raw.Data, m); err != nil {
				l.WithError(err).Warn("Failed to unmarshal metric, skipping it")
				parseFailed++
				continue
			}
			l = l.WithField("metric_name", m.Name)
			tags, fields := parseDims(m.Dims, l)
			fields["value"] = m.Value

			// now add it to the batch
			pt, err := client.NewPoint(m.Name, tags, fields, m.Timestamp)
			if err != nil {
				l.WithError(err).Warn("Failed to create measurement")
				parseFailed++
				continue
			}
			bp.AddPoint(pt)
			parseSuccess++
		}
		parseDur, _ := parsingTimer.Stop(nil)

		log.WithFields(logrus.Fields{
			"incoming_batch_size": parseFailed + parseSuccess,
			"failed_parsing":      parseFailed,
			"outgoing_batch_size": len(bp.Points()),
		}).Debug("Parsed batch, sending it to influx")
		metrics.NewCounter("doppler.influx.incoming_batch_size", nil).CountN(int64(parseFailed+parseSuccess), nil)
		metrics.NewCounter("doppler.influx.outgoing_batch_size", nil).CountN(int64(len(bp.Points())), nil)
		writeDur, err := metrics.TimeBlockErr("doppler.influx.batch_write_dur", nil, func() error {
			return c.Write(bp)
		})
		if err != nil {
			metrics.Count("doppler.influx.failed_batch", nil)
			log.WithError(err).Warn("Failed to write batch to influx")
		}
		dur, _ := timer.Stop(nil)

		log.WithFields(logrus.Fields{
			"parsing_dur": parseDur.Nanoseconds(),
			"write_dur":   writeDur.Nanoseconds(),
			"total_dur":   dur.Nanoseconds(),
		}).Infof("Finished writing batch in %s", dur.String())
	}
}

func parseDims(dims map[string]interface{}, log *logrus.Entry) (map[string]string, map[string]interface{}) {
	tags := map[string]string{}
	fields := map[string]interface{}{}
	for k, v := range dims {
		switch v.(type) {
		case int, int32, int64:
			fields[k] = v
		case string:
			tags[k] = v.(string)
		case float32, float64:
			fields[k] = v
		case bool:
			fields[k] = v
		default:
			log.Debug("unsupported type for %s: %s", k, reflect.TypeOf(v).String())
		}
	}
	return tags, fields
}
