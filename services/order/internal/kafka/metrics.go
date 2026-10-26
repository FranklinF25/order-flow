package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/order/internal/metrics"
)

// WithMetrics instruments the OUTERMOST handler (composition:
// WithMetrics → WithFastGate → WithRetries → Chain) with the pinned
// orderflow_events_total counter: one increment per handled record, exactly
// at the offset-commit decision (handler returned nil ⇒ the record commits;
// an error keeps it uncommitted for redelivery and counts nothing).
//
// Outcome classification (M3 contract: processed|absorbed):
//   - absorbed — the fast-gate hit the replay window, the domain absorbed the
//     record (DB-guard duplicate or compensation no-op), a DLQ park set the
//     record aside, or no subscribed event_type matched (pass-through);
//   - processed — a subscribed handler ran the record to a state change.
//
// The per-record classification travels in the context carrier
// (metrics.EventOutcome); gate/domain/retry layers mark it, this wrapper
// reads it. Nothing else in the stack changes semantics: a marked record
// still returns nil and commits exactly as before.
func WithMetrics(m *metrics.Metrics, group string, next Handler) Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		o := metrics.NewEventOutcome()
		err := next(metrics.ContextWithOutcome(ctx, o), rec)
		if err != nil {
			return err // not committed; the redelivery re-runs the path
		}
		outcome := metrics.OutcomeProcessed
		if o.Absorbed() || !o.Matched() {
			outcome = metrics.OutcomeAbsorbed
		}
		m.IncEvents(group, rec.Topic, headerValue(rec, "event_type"), outcome)
		return nil
	}
}
