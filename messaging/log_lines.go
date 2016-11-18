package messaging

import (
	"fmt"
	"strings"
	"time"

	"encoding/json"

	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/rybit/nats_metrics"
)

type LogMessage struct {
	Timestamp time.Time
	Msg       string
	Hostname  string
	Level     string
	Dims      metrics.DimMap
}

func ExtractLogMsg(data []byte, log *logrus.Entry) (*LogMessage, error) {
	payload := make(map[string]interface{})
	if err := json.Unmarshal(data, payload); err != nil {
		return nil, err
	}

	result := &LogMessage{
		Level:     "unknown",
		Timestamp: time.Now(),
		Dims:      make(map[string]interface{}),
	}

	for k, v := range payload {
		k = strings.ToLower(k)
		switch k {
		case "msg", "@msg":
			if val, ok := v.(string); ok {
				val = strings.TrimSpace(val)
				if len(val) == 0 {
					log.Warn("Failed to parse message found at key '%s': '%v'", k, v)
				} else {
					result.Msg = val
				}
			}
		case "time", "@timestamp", "@time", "timestamp", "_timestamp":
			if ts, err := extractTimestamp(v); err == nil {
				result.Timestamp = ts
			} else {
				log.WithError(err).Warn("Failed to parse timestamp found at key '%s': '%v'", k, v)
			}
		case "level":
			if val, ok := v.(string); ok {
				result.Level = val
			}
		case "hostname", "host":
			if val, ok := v.(string); ok {
				result.Hostname = val
			}
		default:
			payload[k] = v
		}
	}

	// validate the message
	if result.Hostname == "" {
		return nil, errors.New("Failed to parse message because required field hostname is missing. It should be under key 'host' or 'hostname'")
	}
	if result.Msg == "" {
		return nil, errors.New("Failed to parse message because required field msg is missing. It should be under key 'msg' or '@msg'")
	}

	return result, nil
}

func extractTimestamp(face interface{}) (time.Time, error) {
	if unix, ok := face.(int); ok {
		return time.Unix(int64(unix), 0), nil
	}

	if unix, ok := face.(int64); ok {
		return time.Unix(unix, 0), nil
	}

	if str, ok := face.(string); ok {

		for _, format := range []string{time.RFC822Z, time.RFC822} {
			if parsed, err := time.Parse(format, str); err == nil {
				return parsed, nil
			}
		}
	}

	return time.Time{}, fmt.Errorf("Failed to parse '%v' into a timestamp", face)
}
