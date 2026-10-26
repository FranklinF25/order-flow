// Package metrics is the notification service's embedded copy of the pinned
// M3 orderflow metric contract (spec odd/tasks/order-flow-m3.md, S5: identical
// names/labels across services so dashboards and alert rules are written
// once). The service label is the constant "notification".
//
// Served on the EXISTING httpapi mux at GET /metrics (no new port, no new env).
// The mailer binary (cmd/mailer) has no HTTP server and stays uninstrumented.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServiceName is the constant value of the service label on every metric.
const ServiceName = "notification"

// Outcome label values for orderflow_events_total (pinned contract).
const (
	OutcomeProcessed = "processed"
	OutcomeAbsorbed  = "absorbed"
)

// reg holds only the pinned orderflow metrics; /metrics serves exactly this
// registry (dashboards/alerts are authored against the contract, nothing else).
var reg = prometheus.NewRegistry()

var (
	// EventsTotal — orderflow_events_total{service,group,topic,event_type,outcome}.
	EventsTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_events_total",
		Help: "Order.events records handled, by consumer group, topic, event type and outcome (processed | absorbed).",
	}, []string{"service", "group", "topic", "event_type", "outcome"})

	// RetriesTotal — orderflow_retries_total{service,group}.
	RetriesTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_retries_total",
		Help: "Blocked handler retries (backoff scheduled) by consumer group.",
	}, []string{"service", "group"})

	// DLQParkedTotal — orderflow_dlq_parked_total{service,group,topic}.
	DLQParkedTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_dlq_parked_total",
		Help: "Records parked on <topic>.<group>.dlq after the retry budget (parent of alert S4).",
	}, []string{"service", "group", "topic"})

	// OutboxPending — orderflow_outbox_pending{service}.
	OutboxPending = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        "orderflow_outbox_pending",
		Help:        "Unpublished outbox rows (published_at IS NULL), refreshed each relay tick (parent of the L6 head-of-line alert).",
		ConstLabels: prometheus.Labels{"service": ServiceName},
	})

	// OutboxPublishedTotal — orderflow_outbox_published_total{service,topic}.
	OutboxPublishedTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_outbox_published_total",
		Help: "Outbox events successfully published by the relay.",
	}, []string{"service", "topic"})

	// NotificationsPublishedTotal — orderflow_notifications_published_total{kind}.
	NotificationsPublishedTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_notifications_published_total",
		Help: "RabbitMQ publishes on orderflow.notifications by routing-key kind (order.confirmed / order.cancelled).",
	}, []string{"kind"})
)

// Zero-initialize the known label combinations so every pinned series exists
// from the first scrape — dashboards and alert rules (S3/S4) are authorable
// before the first event flows. Dynamically-labeled series (e.g. pass-through
// event_type values) appear when first observed.
func init() {
	for _, et := range []string{"orderflow.order.OrderConfirmed", "orderflow.order.OrderCancelled"} {
		for _, outcome := range []string{OutcomeProcessed, OutcomeAbsorbed} {
			EventsTotal.WithLabelValues(ServiceName, "notification.order-events", "order.events", et, outcome)
		}
	}
	RetriesTotal.WithLabelValues(ServiceName, "notification.order-events")
	DLQParkedTotal.WithLabelValues(ServiceName, "notification.order-events", "order.events")
	OutboxPending.Set(0)
	OutboxPublishedTotal.WithLabelValues(ServiceName, "notification.events")
	NotificationsPublishedTotal.WithLabelValues("order.confirmed")
	NotificationsPublishedTotal.WithLabelValues("order.cancelled")
}

// Handler exposes the pinned registry for the existing httpapi mux.
func Handler() http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// IncEvents counts one handled order.events record after its offset-commit
// decision: outcome=processed when the business effect applied, absorbed on
// duplicate replays, non-matching pass-throughs and DLQ parks.
func IncEvents(group, topic, eventType, outcome string) {
	EventsTotal.WithLabelValues(ServiceName, group, topic, eventType, outcome).Inc()
}

// IncRetries counts one blocked retry attempt (backoff scheduled before the
// next in-process attempt).
func IncRetries(group string) {
	RetriesTotal.WithLabelValues(ServiceName, group).Inc()
}

// IncDLQParked counts one record parked on <topic>.<group>.dlq.
func IncDLQParked(group, topic string) {
	DLQParkedTotal.WithLabelValues(ServiceName, group, topic).Inc()
}

// SetOutboxPending refreshes the unpublished outbox row count each relay tick.
func SetOutboxPending(n float64) { OutboxPending.Set(n) }

// AddOutboxPublished counts n outbox events successfully published by the
// relay on the given topic.
func AddOutboxPublished(topic string, n int) {
	OutboxPublishedTotal.WithLabelValues(ServiceName, topic).Add(float64(n))
}

// IncNotificationPublished counts one successful RabbitMQ publish by its
// routing-key kind (order.confirmed / order.cancelled).
func IncNotificationPublished(kind string) {
	NotificationsPublishedTotal.WithLabelValues(kind).Inc()
}
