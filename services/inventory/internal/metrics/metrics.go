// Package metrics is the embedded observability module of the inventory
// service (M3 spec S5): the pinned order-flow metric contract
// (odd/tasks/order-flow-m3.md, "Metric contract") with EXACT names/labels so
// dashboards and alert rules are written once across services, served on the
// EXISTING httpapi mux at GET /metrics (no new ports, no new env).
//
// The service label is the constant "inventory" on every metric; the helper
// functions are the only write surface, so call sites cannot mistype a label.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ServiceName is the constant service label of every inventory metric.
const ServiceName = "inventory"

// Outcome labels of orderflow_events_total (pinned vocabulary).
const (
	OutcomeProcessed = "processed" // the record applied a domain fact
	OutcomeAbsorbed  = "absorbed"  // the record was a no-op (duplicate, unknown order, already-RELEASED, pass-through event_type)
)

// Pinned collectors (default registry; promhttp serves it on GET /metrics).
var (
	// EventsTotal is orderflow_events_total{service,group,topic,event_type,outcome}.
	EventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_events_total",
		Help: "Consumed records handled to an outcome (processed = fact applied, absorbed = no-op); incremented after the offset-commit decision.",
	}, []string{"service", "group", "topic", "event_type", "outcome"})

	// RetriesTotal is orderflow_retries_total{service,group}.
	RetriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_retries_total",
		Help: "Blocking retry attempts started after a failed handler attempt (topology §7 in-process budget).",
	}, []string{"service", "group"})

	// DlqParkedTotal is orderflow_dlq_parked_total{service,group,topic}.
	DlqParkedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_dlq_parked_total",
		Help: "Records parked on <topic>.<group>.dlq after the retry budget was exhausted (parent of the M3 DLQ alert).",
	}, []string{"service", "group", "topic"})

	// OutboxPending is orderflow_outbox_pending{service}.
	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "orderflow_outbox_pending",
		Help: "Unpublished outbox rows (published_at IS NULL), refreshed on every relay tick (parent of the head-of-line alert).",
		ConstLabels: prometheus.Labels{"service": ServiceName},
	})

	// OutboxPublishedTotal is orderflow_outbox_published_total{service,topic}.
	OutboxPublishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orderflow_outbox_published_total",
		Help: "Outbox events successfully produced to Kafka (broker-acked), per topic.",
	}, []string{"service", "topic"})
)

// IncEvent counts one handled record on orderflow_events_total.
func IncEvent(group, topic, eventType, outcome string) {
	EventsTotal.WithLabelValues(ServiceName, group, topic, eventType, outcome).Inc()
}

// IncRetry counts one blocking retry attempt on orderflow_retries_total.
func IncRetry(group string) {
	RetriesTotal.WithLabelValues(ServiceName, group).Inc()
}

// IncDlqParked counts one DLQ park on orderflow_dlq_parked_total.
func IncDlqParked(group, topic string) {
	DlqParkedTotal.WithLabelValues(ServiceName, group, topic).Inc()
}

// SetOutboxPending refreshes orderflow_outbox_pending from the relay tick's
// COUNT(*) of unpublished outbox rows.
func SetOutboxPending(n int64) {
	OutboxPending.Set(float64(n))
}

// IncOutboxPublished counts one broker-acked outbox publish on
// orderflow_outbox_published_total.
func IncOutboxPublished(topic string) {
	OutboxPublishedTotal.WithLabelValues(ServiceName, topic).Inc()
}
