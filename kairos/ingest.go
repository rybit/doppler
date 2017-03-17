package kairos

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"strings"

	"reflect"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/rybit/doppler/messaging"
	"github.com/rybit/nats_metrics"
)

const nanoInMilli int64 = 1000000

func ProcessMetrics(nc *nats.Conn, log *logrus.Entry, config *Config) error {
	if config.MetricsConf == nil {
		return errors.New("Must provide a metrics configuration")
	}

	if config.Addr == "" {
		return errors.New("Must provide an address for kiaros")
	}

	client := NewHTTPClient(&config.HTTPConfig)
	log.WithField("client_addr", config.Addr).Debug("Built http client")
	handler := buildHandler(client, config.MetricsConf)

	sub, wg, err := messaging.ConsumeInBatches(nc, log, config.MetricsConf, handler)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	wg.Wait()
	return nil
}

func buildHandler(client *HTTPClient, config *messaging.IngestConfig) messaging.BatchHandler {
	return func(batch map[time.Time]*nats.Msg, log *logrus.Entry) {
		start := time.Now()
		batchTimer := metrics.NewTimer("doppler.kairos.batch_dur", nil)
		log = log.WithFields(logrus.Fields{
			"batch_id": fmt.Sprintf("%d", start.Nanosecond()),
		})

		payload := []Point{}
		var parseFailed int64
		var missingDims int64
		var metricsSeen int64
		parsingDur := metrics.TimeBlock("doppler.kairos.parsing_dur", nil, func() {
			for _, raw := range batch {

				l := log.WithField("subject", raw.Subject)
				incomingMetric := new(metrics.RawMetric)
				if err := json.Unmarshal(raw.Data, incomingMetric); err != nil {
					l.WithError(err).Warn("Failed to unmarshal metric, skipping it")
					parseFailed++
					continue
				}

				if len(incomingMetric.Dims) == 0 {
					l.Warnf("Skipping metric '%s' because it doesn't have any dimensions and kairos would reject it", incomingMetric.Name)
					missingDims++
					continue
				}

				l = l.WithField("metric_name", incomingMetric.Name)
				outMetric := Point{
					Value: incomingMetric.Value,
					Name:  incomingMetric.Name,
					// kairos takes milliseconds since epoch
					Timestamp: incomingMetric.Timestamp.UnixNano() / nanoInMilli,
					Tags:      make(map[string]interface{}),
				}

				for k, v := range incomingMetric.Dims {
					switch v.(type) {
					case string:
						s := strings.TrimSpace(v.(string))
						if s == "" {
							l.WithField("dimension", k).Debugf("Skipping dimension %s because it is an empty string", k)
						} else {
							outMetric.Tags[k] = v
						}
					case int, int32, int64, float32, float64, bool:
						outMetric.Tags[k] = v
					default:
						l.WithField("dimension", k).Warnf("Skipping dimension %s because it is a %s", k, reflect.TypeOf(v))
					}
				}
				payload = append(payload, outMetric)
			}
		})

		log.WithFields(logrus.Fields{
			"parsing_dur": parsingDur.Nanoseconds(),
		}).Debug("Parsed batch, sending it to kairos")
		metrics.NewCounter("doppler.kairos.incoming_batch_size", nil).CountN(metricsSeen, nil)
		metrics.NewCounter("doppler.kairos.outgoing_batch_size", nil).CountN(int64(len(payload)), nil)
		if missingDims > 0 {
			metrics.NewCounter("doppler.kairos.missing_dims_size", nil).CountN(missingDims, nil)
		}
		if parseFailed > 0 {
			metrics.NewCounter("doppler.kairos.failed_parsing_size", nil).CountN(parseFailed, nil)
		}

		writeDur, err := metrics.TimeBlockErr("doppler.kairos.write_dur", nil, func() error {
			return client.AddPoints(payload)
		})
		if err != nil {
			log.WithError(err).Warn("Failed to write batch to kairos")
			metrics.Count("doppler.kairos.failed_batch", nil)
		} else {
			metrics.Count("doppler.kairos.successful_batch", nil)
		}

		batchDur, _ := batchTimer.Stop(nil)
		log.WithFields(logrus.Fields{
			"incoming_batch_size": metricsSeen,
			"failed_parsing":      parseFailed,
			"missing_dims":        missingDims,
			"outgoing_batch_size": len(payload),
			"parsing_dur":         parsingDur.Nanoseconds(),
			"write_dur":           writeDur.Nanoseconds(),
			"total_dur":           batchDur.Nanoseconds(),
		}).Infof("Finished writing batch in %s", batchDur.String())
	}
}
