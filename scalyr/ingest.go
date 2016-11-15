package scalyr

import (
	"strings"

	"reflect"

	"fmt"

	"net/http"

	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/api/types/time"
	"github.com/nats-io/nats"
	"github.com/pborman/uuid"
	"github.com/rybit/doppler/messaging"
)

type scalyrEventType int
type scalyrSeverity int

const (
	scalyrIngestURL = "https://www.scalyr.com/addEvents"
	maxPostSize     = 3000000

	normalEventType scalyrEventType = 0
	startEventType  scalyrEventType = 1
	endEventType    scalyrEventType = 2

	finestSeverity scalyrSeverity = 0
	finerSeverity  scalyrSeverity = 1
	fineSeverity   scalyrSeverity = 2
	infoSeverity   scalyrSeverity = 3
	warnSeverity   scalyrSeverity = 4
	errorSeverity  scalyrSeverity = 5
	fatalSeverity  scalyrSeverity = 6
)

type ScalyrConfig struct {
	Token string `mapstructure:"token"`

	BatchTimeout int `mapstructure:"batch_timeout"`
	BatchSize    int `mapstructure:"batch_size"`

	Subject    string `mapstructure:"subject"`
	Group      string `mapstructure:"group"`
	PoolSize   int    `mapstructure:"pool_size"`
	BufferSize int    `mapstructure:"buffer_size"`
}

func ProcessLogsToScalyr(nc *nats.Conn, log *logrus.Entry, config *ScalyrConfig) error {
	var sessionID = uuid.NewRandom().String()

	shared := make(chan *nats.Msg, config.BufferSize)
	wg, err := messaging.BuildBatchingWorkerPool(
		shared,
		config.PoolSize,
		config.BatchSize,
		config.BatchTimeout,
		log.WithField("session_id", sessionID),
		buildHandler(config),
	)
	if err != nil {
		return err
	}

	sub, err := messaging.BufferedSubscribe(shared, nc, config.Subject, config.Group)
	if err != nil {
		return err
	}

	defer sub.Unsubscribe()

	wg.Wait()
	return nil
}

type scalyrEvent struct {
	Timestamp string                 `json:"ts"`
	Type      scalyrEventType        `json:"type"`
	Severity  scalyrSeverity         `json:"sev"`
	Thread    string                 `json:"thread"`
	Attrs     map[string]interface{} `json:"attrs"`
}

func buildHandler(config *ScalyrConfig) messaging.BatchHandler {
	baseID := uuid.NewRandom().String()
	client := http.Client{}
	type parsedBatch struct {
		threads   map[string]bool
		events    []scalyrEvent
		hostname  string
		sessionID string
	}

	// we have to write batches serially because the timestamps on a given session ID
	// need to be always increasing. Meaning we can't write multiple batches over the
	// wire at the same time. Batches are also per host so that scalyr can map host <-> streams
	// we will generate a sessionID per host/stream
	batches := make(chan parsedBatch)

	return func(batch map[time.Time]*nats.Msg, log *logrus.Entry) {
		perSession := make(map[string]*parsedBatch)

		//threads := map[string]bool{}
		//sessions := map[string][]scalyrEvent[]{}

		for _, raw := range batch {
			log := log.WithField("source", raw.Subject)

			msg, err := messaging.ExtractLogMsg(raw.Data, log)
			if err != nil {
				log.WithError(err).Warn("Failed to parse log line - skipping it")
				continue
			}

			// now get the batch this is going to be a part of
			id := sessionID(baseID, msg.Hostname, raw.Subject)
			log = log.WithField("session_id", id)
			batch := perSession[id]
			if batch == nil {
				batch = *parsedBatch{
					sessionID: id,
					hostname:  msg.Hostname,
					threads:   map[string]bool{},
					events:    []scalyrEvent{},
				}
			}

			// build the actual event
			attrs := map[string]interface{}{
				"message":  msg.Msg,
				"hostname": msg.Hostname,
			}
			for k, v := range msg.Dims {
				if onlyStringOrNumber(v) {
					attrs[k] = v
				} else {
					log.Warnf("Unsupported type for dim: %s, type: %s, value: %v", k, reflect.TypeOf(v).String(), v)
				}
			}

			batch.events = append(batch.events, scalyrEvent{
				Severity:  severity(msg.Level),
				Attrs:     attrs,
				Timestamp: fmt.Sprintf("%d", msg.Timestamp.UnixNano()),
				Type:      normalEventType,
				Thread:    raw.Subject,
			})
			batch.threads[raw.Subject] = true
		}

		log.Debug("Finished parsing batch of log messages into %d scalyr batches", len(perSession))

		for _, b := range perSession {
			batches <- b // TODO
		}
	}
}

func severity(raw string) scalyrSeverity {
	switch strings.ToLower(raw) {
	case "finest":
		return finestSeverity
	case "finer":
		return finerSeverity
	case "fine", "debug", "debu":
		return fineSeverity
	case "info":
		return infoSeverity
	case "warn", "warning":
		return warnSeverity
	case "err", "error", "erro":
		return errorSeverity
	case "fatal", "fata", "fatl", "panic":
		return fatalSeverity
	}
	return infoSeverity
}

func onlyStringOrNumber(face interface{}) bool {
	switch face.(type) {
	case int, int32, int64, float32, float64, string:
		return true
	}
	return false
}

func sessionID(baseID, hostname, stream string) string {
	return fmt.Sprintf("%d/%d/%d", baseID, hostname, stream)
}
