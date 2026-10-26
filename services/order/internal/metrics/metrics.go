// Package metrics owns the pinned orderflow observability contract (M3, S5):
// every service embeds this same file pattern with IDENTICAL metric names and
// labels so dashboards and alert rules are authored once. The service label
// is the constant "order" here. The registry is private and served on the
// EXISTING httpapi mux at GET /metrics (no new ports, no go-runtime noise).
package metrics

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServiceName is the pinned service label value for this service.
const ServiceName = "order"

// Pinned outcomes for orderflow_events_total (M3 metric contract).
const (
	OutcomeProcessed = "processed"
	OutcomeAbsorbed  = "absorbed"
)

// Metrics owns every pinned orderflow collector plus the private registry
// they are registered on. Collector fields are exported so tests assert
// label values directly via prometheus/client_golang/prometheus/testutil.
type Metrics struct {
	EventsTotal     *prometheus.CounterVec // orderflow_events_total{service,group,topic,event_type,outcome}
	RetriesTotal    *prometheus.CounterVec // orderflow_retries_total{service,group}
	DLQParkedTotal  *prometheus.CounterVec // orderflow_dlq_parked_total{service,group,topic}
	OutboxPending   *prometheus.GaugeVec   // orderflow_outbox_pending{service}
	OutboxPublished *prometheus.CounterVec // orderflow_outbox_published_total{service,topic}
	SagaOrders      *prometheus.GaugeVec   // orderflow_saga_orders{status}

	reg *prometheus.Registry
}

// New builds the collectors and registers them on a private registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	m := &Metrics{
		EventsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "orderflow_events_total",
			Help: "Consumed records handled to an offset-commit decision, by outcome (processed|absorbed).",
		}, []string{"service", "group", "topic", "event_type", "outcome"}),
		RetriesTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "orderflow_retries_total",
			Help: "Blocked handler attempts that entered the §7 exponential backoff.",
		}, []string{"service", "group"}),
		DLQParkedTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "orderflow_dlq_parked_total",
			Help: "Records parked on a dead-letter topic after the retry budget was exhausted (S4 alert parent).",
		}, []string{"service", "group", "topic"}),
		OutboxPending: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "orderflow_outbox_pending",
			Help: "Unpublished outbox rows (published_at IS NULL), refreshed on every relay tick (L6 head-of-line alert parent).",
		}, []string{"service"}),
		OutboxPublished: f.NewCounterVec(prometheus.CounterOpts{
			Name: "orderflow_outbox_published_total",
			Help: "Outbox events successfully published to Kafka (produce acked).",
		}, []string{"service", "topic"}),
		SagaOrders: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "orderflow_saga_orders",
			Help: "Live count of orders per status, refreshed by the bounded saga poller (≤15s).",
		}, []string{"status"}),
		reg: reg,
	}
	m.seed()
	return m
}

// seed materializes each family's zero-valued children with the service's
// REAL label values (its two consumer groups and subscribed event types, the
// produced topic, the four saga statuses — mirroring order.Statuses, which
// this package cannot import without a cycle: order imports metrics for the
// outcome carrier). Prometheus vecs expose no family until a child exists;
// seeding makes the full pinned contract visible from boot at 0 instead of
// materializing lazily, so dashboards and /metrics checks see every series.
func (m *Metrics) seed() {
	consumed := []struct{ group, topic, eventType string }{
		{"order.inventory-events", "inventory.events", "StockReserved"},
		{"order.inventory-events", "inventory.events", "StockUnavailable"},
		{"order.payment-events", "payment.events", "PaymentCaptured"},
		{"order.payment-events", "payment.events", "PaymentFailed"},
	}
	for _, c := range consumed {
		for _, o := range []string{OutcomeProcessed, OutcomeAbsorbed} {
			m.EventsTotal.WithLabelValues(ServiceName, c.group, c.topic, c.eventType, o)
		}
		m.RetriesTotal.WithLabelValues(ServiceName, c.group)
		m.DLQParkedTotal.WithLabelValues(ServiceName, c.group, c.topic)
	}
	m.OutboxPending.WithLabelValues(ServiceName)
	m.OutboxPublished.WithLabelValues(ServiceName, "order.events")
	for _, s := range []string{"AWAITING_STOCK", "AWAITING_PAYMENT", "CONFIRMED", "CANCELLED"} {
		m.SagaOrders.WithLabelValues(s)
	}
}

// Handler serves this registry on the existing httpapi mux (GET /metrics).
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// IncEvents counts one handled record at its offset-commit decision.
func (m *Metrics) IncEvents(group, topic, eventType, outcome string) {
	m.EventsTotal.WithLabelValues(ServiceName, group, topic, eventType, outcome).Inc()
}

// IncRetries counts one blocked retry attempt: a failed attempt that enters
// the §7 backoff. The final (parking) attempt is not a retry.
func (m *Metrics) IncRetries(group string) {
	m.RetriesTotal.WithLabelValues(ServiceName, group).Inc()
}

// IncDLQParked counts one record parked on the group's DLQ for a source topic.
func (m *Metrics) IncDLQParked(group, topic string) {
	m.DLQParkedTotal.WithLabelValues(ServiceName, group, topic).Inc()
}

// SetOutboxPending refreshes the unpublished-outbox gauge on a relay tick.
func (m *Metrics) SetOutboxPending(n float64) {
	m.OutboxPending.WithLabelValues(ServiceName).Set(n)
}

// IncOutboxPublished counts one produce-acked outbox event.
func (m *Metrics) IncOutboxPublished(topic string) {
	m.OutboxPublished.WithLabelValues(ServiceName, topic).Inc()
}

// EventOutcome is the per-record classification carrier behind
// orderflow_events_total. The instrumented consumer places one in the
// record's context; the layers that KNOW a record was absorbed (fast-gate
// hit, pass-through dispatch, domain duplicate/compensation no-op) mark it,
// and the outermost wrapper reads it after the offset-commit decision. It is
// pure plumbing — no counters here, and every entry point is nil-safe so
// layers behave identically when invoked outside the instrumented consumer.
type EventOutcome struct {
	mu       sync.Mutex
	absorbed bool
	matched  bool
}

type outcomeKey struct{}

// NewEventOutcome builds the classification carrier for one record.
func NewEventOutcome() *EventOutcome { return &EventOutcome{} }

// ContextWithOutcome returns ctx carrying o.
func ContextWithOutcome(ctx context.Context, o *EventOutcome) context.Context {
	return context.WithValue(ctx, outcomeKey{}, o)
}

func outcomeFrom(ctx context.Context) *EventOutcome {
	o, _ := ctx.Value(outcomeKey{}).(*EventOutcome)
	return o
}

// MarkAbsorbed records that this record was a safe no-op (gate hit, DB-guard
// duplicate, compensation absorb). Nil-carrier safe: the domain layer calls
// it with whatever context it received.
func MarkAbsorbed(ctx context.Context) {
	if o := outcomeFrom(ctx); o != nil {
		o.mu.Lock()
		o.absorbed = true
		o.mu.Unlock()
	}
}

// MarkMatched records that a subscribed handler claimed the record (its
// event_type header filter passed); unmatched records are pass-throughs.
func MarkMatched(ctx context.Context) {
	if o := outcomeFrom(ctx); o != nil {
		o.mu.Lock()
		o.matched = true
			o.mu.Unlock()
	}
}

// Absorbed reports whether any layer marked the record as a safe no-op.
func (o *EventOutcome) Absorbed() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.absorbed
}

// Matched reports whether a subscribed handler claimed the record.
func (o *EventOutcome) Matched() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.matched
}

// SagaStatusFetcher supplies live order counts per status from ONE grouped
// COUNT query; postgres.Store.CountOrdersByStatus implements it.
type SagaStatusFetcher func(ctx context.Context) (map[string]int64, error)

// RefreshSagaOrders applies one poll: every known status is zeroed first so
// the gauge label set is stable, then live counts overwrite.
func (m *Metrics) RefreshSagaOrders(ctx context.Context, statuses []string, fetch SagaStatusFetcher) error {
	for _, s := range statuses {
		m.SagaOrders.WithLabelValues(s).Set(0)
	}
	counts, err := fetch(ctx)
	if err != nil {
		return fmt.Errorf("saga poll: %w", err)
	}
	for _, s := range statuses {
		m.SagaOrders.WithLabelValues(s).Set(float64(counts[s]))
	}
	return nil
}

// RunSagaOrdersPoller is the bounded poller (interval ≤ 15s per the M3
// contract): one grouped COUNT per tick until ctx is cancelled. The first
// refresh runs immediately so dashboards have data without waiting a full
// interval; poll errors only log — the next tick retries.
func (m *Metrics) RunSagaOrdersPoller(ctx context.Context, statuses []string, interval time.Duration, fetch SagaStatusFetcher) {
	if err := m.RefreshSagaOrders(ctx, statuses, fetch); err != nil {
		log.Printf("saga poller: %v", err)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := m.RefreshSagaOrders(ctx, statuses, fetch); err != nil {
				if ctx.Err() == nil {
					log.Printf("saga poller: %v", err)
				}
			}
		}
	}
}
