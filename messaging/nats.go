package messaging

import (
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"

	"github.com/rybit/doppler/tls"
)

type NatsConfig struct {
	TLSConfig *tls.Config `mapstructure:"tls_conf"`

	Servers []string `mapstructure:"servers"`

	MetricsSubject string `mapstructure:"metrics_subject"`
	LogsSubject    string `mapstructure:"log_subject"`
}

// ServerString will build the proper string for nats connect
func (config *NatsConfig) ServerString() string {
	return strings.Join(config.Servers, ",")
}

// ConnectToNats will do a TLS connection to the nats servers specified
func ConnectToNats(config *NatsConfig, errHandler nats.ErrHandler) (*nats.Conn, error) {
	options := []nats.Option{}
	if config.TLSConfig != nil {
		tlsConfig, err := config.TLSConfig.TLSConfig()
		if err != nil {
			return nil, err
		}
		options = append(options, nats.Secure(tlsConfig))
	}

	if errHandler != nil {
		options = append(options, nats.ErrorHandler(errHandler))
	}

	return nats.Connect(config.ServerString(), options...)
}

func ErrorHandler(log *logrus.Entry) nats.ErrHandler {
	errLogger := log.WithField("component", "error-logger")
	return func(conn *nats.Conn, sub *nats.Subscription, err error) {
		errLogger.WithError(err).WithFields(logrus.Fields{
			"subject":     sub.Subject,
			"group":       sub.Queue,
			"conn_status": conn.Status(),
		}).Error("Error while consuming from " + sub.Subject)
	}
}
