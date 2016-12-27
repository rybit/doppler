package messaging

import (
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"

	"github.com/rybit/doppler/tls"
)

type NatsConfig struct {
	tls.Config `mapstructure:",squash"`

	Servers []string `mapstructure:"servers"`
}

// ServerString will build the proper string for nats connect
func (config *NatsConfig) ServerString() string {
	return strings.Join(config.Servers, ",")
}

// ConnectToNats will do a TLS connection to the nats servers specified
func ConnectToNats(config *NatsConfig, errHandler nats.ErrHandler) (*nats.Conn, error) {
	tlsConfig, err := config.TLSConfig()
	if err != nil {
		return nil, err
	}

	if errHandler != nil {
		return nats.Connect(config.ServerString(), nats.Secure(tlsConfig), nats.ErrorHandler(errHandler))
	}

	return nats.Connect(config.ServerString(), nats.Secure(tlsConfig))
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
