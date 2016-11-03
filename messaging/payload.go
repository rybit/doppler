package messaging

import "time"

const (
	CounterType MetricType = "counter"
	GaugeType   MetricType = "gauge"
	TimerType   MetricType = "timer"
)

type MetricType string
type Dims map[string]interface{}

type InboundMetric struct {
	Timestamp time.Time  `json:"timestamp"`
	Value     int64      `json:"int64"`
	Name      string     `json:"name"`
	Dims      *Dims      `json:"dims"`
	Type      MetricType `json:"type"`
}
