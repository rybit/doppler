package messaging

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
)

func BufferedSubscribe(msgs chan<- *nats.Msg, nc *nats.Conn, subject, group string) (*nats.Subscription, error) {
	writer := func(m *nats.Msg) {
		msgs <- m
	}

	if group != "" {
		return nc.QueueSubscribe(subject, group, writer)
	}
	return nc.Subscribe(subject, writer)
}

func BuildBatchingWorkerPool(shared chan *nats.Msg, poolSize, batchSize, timeoutSec int, log *logrus.Entry, h BatchHandler) (*sync.WaitGroup, error) {
	wg := new(sync.WaitGroup)

	for i := poolSize; i > 0; i-- {
		l := log.WithField("worker_id", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Info("Starting worker")
			timeout := time.Duration(timeoutSec) * time.Second
			in, _ := StartBatcher(timeout, batchSize, l, h)

			for m := range shared {
				in <- m
			}
		}()
	}

	return wg, nil
}

type BatchHandler func(batch map[time.Time]*nats.Msg, log *logrus.Entry)

var inflight int32

func StartBatcher(timeout time.Duration, batchSize int, log *logrus.Entry, h BatchHandler) (chan<- (*nats.Msg), chan<- bool) {
	batchLock := new(sync.Mutex)
	currentBatch := map[time.Time]*nats.Msg{}

	incoming := make(chan *nats.Msg, batchSize)
	shutdown := make(chan bool)

	wrapped := func(batch map[time.Time]*nats.Msg, log *logrus.Entry) {
		start := time.Now()
		flying := atomic.AddInt32(&inflight, 1)
		l := log.WithFields(logrus.Fields{
			"batch_id": start.Nanosecond(),
		})
		l.WithField("inflight_batches", flying).Info("Starting to process batch")

		h(batch, l)

		flying = atomic.AddInt32(&inflight, -1)
		dur := time.Since(start)
		l.WithFields(logrus.Fields{
			"dur":              dur.Nanoseconds(),
			"inflight_batches": flying,
		}).Infof("Finished batch in %s", dur.String())
	}

	go func() {
		ticker := time.Tick(timeout)
		for {
			select {
			case m := <-incoming:
				batchLock.Lock()
				now := time.Now()
				if _, exists := currentBatch[now]; exists {
					log.Warn("Going too fast! There is already a message at %s", now.String())
				}
				currentBatch[now] = m

				if len(currentBatch) > batchSize {
					out := currentBatch
					currentBatch = make(map[time.Time]*nats.Msg)
					go wrapped(out, log.WithField("reason", "size"))
				}
				batchLock.Unlock()
			case <-ticker:
				batchLock.Lock()
				if len(currentBatch) > 0 {
					out := currentBatch
					currentBatch = []*nats.Msg{}
					go wrapped(out, log.WithField("reason", "timeout"))
				}
				batchLock.Unlock()

			case <-shutdown:
				log.Debug("Got shutdown signal")
				close(shutdown)
				return
			}
		}
		log.Debug("Shutdown batcher")
	}()

	return incoming, shutdown
}
