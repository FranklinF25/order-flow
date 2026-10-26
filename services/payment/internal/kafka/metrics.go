package kafka

import (
	"context"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/payment/internal/metrics"
)

// Metrics instrumentation of the pinned M3 contract, wired at the
// NewConsumer composition point so every wrapper (fast-gate, retries/DLQ)
// keeps its own signature and contract untouched:
//
//	orderflow_retries_total   — one per blocked attempt: every failed
//	                            dispatch of the handler UNDER WithRetries
//	                            (a poison record burns 3, a transient that
//	                            recovers on attempt 3 burns 2).
//	orderflow_dlq_parked_total — one per successful park: WithRetries is the
//	                            only caller of the DLQ producer, so wrapping
//	                            the producer counts exactly the parks; the
//	                            label is the SOURCE topic (group + topic
//	                            rebuild <topic>.<group>.dlq for alert S4).
//	orderflow_events_total    — counted in Consumer.handle after the
//	                            offset-commit decision (see there).
//
// Deviation (accepted by the task brief): a fast-gate HIT absorbs inside
// WithFastGate before the handler runs, so those replays are NOT counted in
// events_total — the DB duplicate-marker replays, the non-matching
// pass-throughs and every fresh event are.
type metricsDLQ struct {
	inner DLQProducer
	m     *metrics.Metrics
	group string
}

// newMetricsDLQ decorates the DLQ producer with park counting.
func newMetricsDLQ(inner DLQProducer, m *metrics.Metrics, group string) DLQProducer {
	return metricsDLQ{inner: inner, m: m, group: group}
}

// Produce counts a park only after the broker acked it (a failed produce
// leaves the offset uncommitted — the record is not parked yet).
func (d metricsDLQ) Produce(ctx context.Context, rec *kgo.Record) error {
	if err := d.inner.Produce(ctx, rec); err != nil {
		return err
	}
	d.m.IncDLQParked(d.group, sourceTopic(rec.Topic, d.group))
	return nil
}

// Close releases the inner producer.
func (d metricsDLQ) Close() { d.inner.Close() }

// sourceTopic strips the "<group>.dlq" suffix a park appended to the source
// topic (topology §7: <source-topic>.<group>.dlq).
func sourceTopic(dlqTopic, group string) string {
	return strings.TrimSuffix(dlqTopic, "."+group+".dlq")
}

// blockedAttemptHandler counts one blocked attempt per failed dispatch of
// the handler it wraps (the input of WithRetries), so the retries/DLQ
// wrapper keeps its signature and the count rides the existing port.
type blockedAttemptHandler struct {
	next  Handler
	m     *metrics.Metrics
	group string
}

func (h blockedAttemptHandler) handle(ctx context.Context, rec *kgo.Record) error {
	if err := h.next(ctx, rec); err != nil {
		h.m.IncRetries(h.group)
		return err
	}
	return nil
}
