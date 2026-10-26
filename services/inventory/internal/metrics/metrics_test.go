package metrics

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The pinned contract (odd/tasks/order-flow-m3.md, "Metric contract"): the
// inventory service exposes EXACTLY these orderflow_* families with EXACTLY
// these label sets — dashboards and alerts are authored against this surface,
// so any drift here is a contract break, and nothing service-specific may
// leak into the orderflow_ namespace.
func TestPinnedContractExactNamesAndLabels(t *testing.T) {
	resetPinnedCollectors()
	// Materialize one child per family so every vec appears in the gather.
	IncEvent("inventory.order-events", "order.events", "orderflow.order.OrderPlaced", OutcomeProcessed)
	IncRetry("inventory.order-events")
	IncDlqParked("inventory.order-events", "order.events")
	SetOutboxPending(1)
	IncOutboxPublished("inventory.events")

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string][]string{}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "orderflow_") {
			continue // go_*/process_* runtime families are outside the contract
		}
		if len(f.GetMetric()) == 0 {
			continue
		}
		labels := make([]string, 0, len(f.GetMetric()[0].GetLabel()))
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
		slices.Sort(labels)
		got[f.GetName()] = labels
	}
	want := map[string][]string{
		"orderflow_events_total":           {"event_type", "group", "outcome", "service", "topic"},
		"orderflow_retries_total":          {"group", "service"},
		"orderflow_dlq_parked_total":       {"group", "service", "topic"},
		"orderflow_outbox_pending":         {"service"},
		"orderflow_outbox_published_total": {"service", "topic"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("orderflow_* families = %v, want exactly %v (names AND label sets pinned)", got, want)
	}
}

// Each helper moves exactly its pinned collector with the constant service
// label "inventory" — the helpers are the only write surface, so this proves
// the label paths call sites rely on.
// resetPinnedCollectors isolates each test from the package-level globals:
// assertions read absolute values, so children from earlier tests must not
// accumulate.
func resetPinnedCollectors() {
	EventsTotal.Reset()
	RetriesTotal.Reset()
	DlqParkedTotal.Reset()
	OutboxPublishedTotal.Reset()
	SetOutboxPending(0)
}

func TestHelpersMoveThePinnedCollectors(t *testing.T) {
	resetPinnedCollectors()
	if ServiceName != "inventory" {
		t.Fatalf("service label = %q, want the pinned constant inventory", ServiceName)
	}

	IncEvent("inventory.order-events", "order.events", "orderflow.order.OrderPlaced", OutcomeProcessed)
	IncEvent("inventory.order-events", "order.events", "orderflow.order.OrderPlaced", OutcomeAbsorbed)
	IncEvent("inventory.order-events", "order.events", "orderflow.order.OrderPlaced", OutcomeAbsorbed)
	if got := testutil.ToFloat64(EventsTotal.WithLabelValues(ServiceName, "inventory.order-events", "order.events", "orderflow.order.OrderPlaced", OutcomeProcessed)); got != 1 {
		t.Errorf("events_total processed = %v, want 1", got)
	}
	if got := testutil.ToFloat64(EventsTotal.WithLabelValues(ServiceName, "inventory.order-events", "order.events", "orderflow.order.OrderPlaced", OutcomeAbsorbed)); got != 2 {
		t.Errorf("events_total absorbed = %v, want 2", got)
	}

	IncRetry("inventory.order-events")
	IncRetry("inventory.order-events")
	if got := testutil.ToFloat64(RetriesTotal.WithLabelValues(ServiceName, "inventory.order-events")); got != 2 {
		t.Errorf("retries_total = %v, want 2", got)
	}

	IncDlqParked("inventory.order-events", "order.events.inventory.order-events.dlq")
	if got := testutil.ToFloat64(DlqParkedTotal.WithLabelValues(ServiceName, "inventory.order-events", "order.events.inventory.order-events.dlq")); got != 1 {
		t.Errorf("dlq_parked_total = %v, want 1", got)
	}

	SetOutboxPending(7)
	if got := testutil.ToFloat64(OutboxPending); got != 7 {
		t.Errorf("outbox_pending = %v, want 7", got)
	}
	SetOutboxPending(0) // the refresh semantics: an empty backlog must be visible too
	if got := testutil.ToFloat64(OutboxPending); got != 0 {
		t.Errorf("outbox_pending after refresh to 0 = %v, want 0", got)
	}

	IncOutboxPublished("inventory.events")
	if got := testutil.ToFloat64(OutboxPublishedTotal.WithLabelValues(ServiceName, "inventory.events")); got != 1 {
		t.Errorf("outbox_published_total = %v, want 1", got)
	}
}
