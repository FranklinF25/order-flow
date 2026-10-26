// Package metrics is the payment service's embedded copy of the pinned M3
// observability contract (odd/tasks/order-flow-m3.md, "Metric contract";
// S5: identical names/labels per service so dashboards and alerts are written
// once). Exactly five orderflow_* collectors — no service-specific extras:
//
//	orderflow_events_total{service,group,topic,event_type,outcome}
//	orderflow_retries_total{service,group}
//	orderflow_dlq_parked_total{service,group,topic}
//	orderflow_outbox_pending{service}
//	orderflow_outbox_published_total{service,topic}
//
// Served on the EXISTING httpapi mux at GET /metrics (promhttp, default
// registry) — no new port, no new env.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// ServiceName is the constant service label of every payment metric.
const ServiceName = "payment"

// Outcome values of orderflow_events_total (pinned vocabulary).
const (
	OutcomeProcessed = "processed" // the event was handled fresh (DB guard inserted it)
	OutcomeAbsorbed  = "absorbed"  // a replay (duplicate marker / gate-adjacent pass-through) was absorbed
)

// Metrics owns the five pinned collectors.
type Metrics struct {
	events          *prometheus.CounterVec // orderflow_events_total{service,group,topic,event_type,outcome}
	retries         *prometheus.CounterVec // orderflow_retries_total{service,group}
	dlqParked       *prometheus.CounterVec // orderflow_dlq_parked_total{service,group,topic}
	outboxPending   *prometheus.GaugeVec   // orderflow_outbox_pending{service}
	outboxPublished *prometheus.CounterVec // orderflow_outbox_published_total{service,topic}
}

// New builds the metrics on the default registry (what GET /metrics serves).
func New() *Metrics {
	return NewWithRegisterer(prometheus.DefaultRegisterer)
}

// NewWithRegisterer builds the metrics on an arbitrary registry — tests use
// fresh registries so counter movement is isolated per test.
func NewWithRegisterer(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderflow_events_total",
			Help: "Handled consumer records by outcome: processed (fresh) or absorbed (duplicate-marker replay / non-matching pass-through).",
		}, []string{"service", "group", "topic", "event_type", "outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderflow_retries_total",
			Help: "Blocked handler attempts inside the blocking-retry wrapper (1 initial + 2 retries).",
		}, []string{"service", "group"}),
		dlqParked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderflow_dlq_parked_total",
			Help: "Records parked on <topic>.<group>.dlq after exhausting the retry budget (parent of alert S4).",
		}, []string{"service", "group", "topic"}),
		outboxPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "orderflow_outbox_pending",
			Help: "Unpublished outbox rows (published_at IS NULL), refreshed on each relay tick (parent of the L6 head-of-line alert).",
		}, []string{"service"}),
		outboxPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderflow_outbox_published_total",
			Help: "Outbox events successfully published (produced and acked) by the relay.",
		}, []string{"service", "topic"}),
	}
	reg.MustRegister(m.events, m.retries, m.dlqParked, m.outboxPending, m.outboxPublished)
	m.seed()
	return m
}

// seed materializes each family's children with the service's REAL label
// values (its one consumer group and subscribed event type, the produced
// topic — mirroring the NewConsumer/NewRelay wiring, which this embedded
// copy cannot import). Prometheus vecs expose NO family until a child
// exists; seeding makes the full pinned contract visible from boot at 0
// instead of materializing lazily, so dashboards and /metrics checks see
// every series.
func (m *Metrics) seed() {
	const (
		group       = "payment.inventory-events"
		topic       = "inventory.events"
		eventType   = "orderflow.inventory.StockReserved"
		outboxTopic = "payment.events"
	)
	for _, o := range []string{OutcomeProcessed, OutcomeAbsorbed} {
		m.events.WithLabelValues(ServiceName, group, topic, eventType, o)
	}
	m.retries.WithLabelValues(ServiceName, group)
	m.dlqParked.WithLabelValues(ServiceName, group, topic)
	m.outboxPending.WithLabelValues(ServiceName)
	m.outboxPublished.WithLabelValues(ServiceName, outboxTopic)
}

// EventsCounter exposes the events vec (tests assert exact label movement).
func (m *Metrics) EventsCounter() *prometheus.CounterVec { return m.events }

// RetriesCounter exposes the retries vec.
func (m *Metrics) RetriesCounter() *prometheus.CounterVec { return m.retries }

// DLQParkedCounter exposes the DLQ park vec.
func (m *Metrics) DLQParkedCounter() *prometheus.CounterVec { return m.dlqParked }

// OutboxPendingGauge exposes the pending gauge vec (tests read its single
// child's absolute value).
func (m *Metrics) OutboxPendingGauge() *prometheus.GaugeVec { return m.outboxPending }

// OutboxPublishedCounter exposes the published vec.
func (m *Metrics) OutboxPublishedCounter() *prometheus.CounterVec { return m.outboxPublished }

// IncEvents counts one handled record after its offset-commit decision.
func (m *Metrics) IncEvents(group, topic, eventType, outcome string) {
	m.events.WithLabelValues(ServiceName, group, topic, eventType, outcome).Inc()
}

// IncRetries counts one blocked handler attempt inside the retry wrapper.
func (m *Metrics) IncRetries(group string) {
	m.retries.WithLabelValues(ServiceName, group).Inc()
}

// IncDLQParked counts one successful park on <topic>.<group>.dlq.
func (m *Metrics) IncDLQParked(group, topic string) {
	m.dlqParked.WithLabelValues(ServiceName, group, topic).Inc()
}

// SetOutboxPending refreshes the unpublished-outbox gauge (per relay tick).
func (m *Metrics) SetOutboxPending(pending int64) {
	m.outboxPending.WithLabelValues(ServiceName).Set(float64(pending))
}

// IncOutboxPublished counts one successfully published outbox event.
func (m *Metrics) IncOutboxPublished(topic string) {
	m.outboxPublished.WithLabelValues(ServiceName, topic).Inc()
}
