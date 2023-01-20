package stream

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type (
	Option  func(*options)
	options struct {
		heartbeatMillis int64
		maxMailbox      int
		queuingTimeout  time.Duration
		clientReceived  func(stream, code string)
		clientQueued    func(stream, code string)
		clientStarted   func(stream string)
		clientCompleted func(stream, code string, duration int64)
		logger          *zap.Logger
	}
)

func WithHeartbeatMillis(millis int64) Option {
	return func(o *options) {
		o.heartbeatMillis = millis
	}
}

func WithQueuingTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.queuingTimeout = timeout
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithMetrics(r prometheus.Registerer, name string) Option {
	const (
		namespace = "suzujun"
		subsystem = "spanner_stream"
	)
	collectorReceived := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "received_message",
			Help:      "Total number of received messages.",
		}, []string{"name", "stream"},
	)
	collectorQueued := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queued_message",
			Help:      "Total number of queued messages.",
		}, []string{"name", "stream", "code"},
	)
	collectorStarted := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "started_requests_total",
			Help:      "Total number of started requests.",
		},
		[]string{"name", "stream", "code"},
	)
	collectorCompleted := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "completed_requests_total",
			Help:      "Total number of completed requests.",
		},
		[]string{"name", "stream"},
	)
	collectorLatency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "completed_requests_seconds",
			Help:      "Histogram of completed request latency (milliseconds).",
			Buckets: func() []float64 {
				const start, factor, count = 25, 2.0, 8 // from 25ms to 3200ms,
				return prometheus.ExponentialBuckets(start, factor, count)
			}(),
		},
		[]string{"name", "stream", "code"},
	)
	r.MustRegister(collectorReceived, collectorStarted, collectorCompleted, collectorLatency)
	return func(o *options) {
		o.clientReceived = func(stream, code string) {
			collectorReceived.WithLabelValues(name, stream, code).Inc()
		}
		o.clientQueued = func(stream, code string) {
			collectorQueued.WithLabelValues(name, stream, code).Inc()
		}
		o.clientStarted = func(stream string) {
			collectorStarted.WithLabelValues(name, stream).Inc()
		}
		o.clientCompleted = func(stream, code string, duration int64) {
			collectorCompleted.WithLabelValues(name, stream, code).Inc()
			collectorLatency.WithLabelValues(name, stream, code).Observe(float64(duration))
		}
	}
}
