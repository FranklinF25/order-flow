package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

// gatherFamilies materializes one child per family (so empty vecs still show
// their label contract) and returns family name → sorted label names.
func gatherFamilies(t *testing.T) map[string][]string {
	t.Helper()
	// Materialize children directly (independent of the helper bodies).
	EventsTotal.WithLabelValues("notification", "g", "t", "et", "processed")
	RetriesTotal.WithLabelValues("notification", "g")
	DLQParkedTotal.WithLabelValues("notification", "g", "t")
	OutboxPending.Set(0)
	OutboxPublishedTotal.WithLabelValues("notification", "t")
	NotificationsPublishedTotal.WithLabelValues("order.confirmed")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string][]string{}
	for _, mf := range families {
		if len(mf.Metric) == 0 {
			continue
		}
		var names []string
		for _, l := range mf.Metric[0].Label {
			names = append(names, l.GetName())
		}
		sort.Strings(names)
		out[mf.GetName()] = names
	}
	return out
}

// The pinned contract (spec odd/tasks/order-flow-m3.md): EXACT names and
// labels, shared across services so dashboards/alerts are written once (S5).
func TestPinnedMetricContract(t *testing.T) {
	families := gatherFamilies(t)
	want := map[string][]string{
		"orderflow_events_total":                  {"event_type", "group", "outcome", "service", "topic"},
		"orderflow_retries_total":                 {"group", "service"},
		"orderflow_dlq_parked_total":              {"group", "service", "topic"},
		"orderflow_outbox_pending":                {"service"},
		"orderflow_outbox_published_total":        {"service", "topic"},
		"orderflow_notifications_published_total": {"kind"},
	}
	for name, labels := range want {
		got, ok := families[name]
		if !ok {
			t.Errorf("metric family %s missing from the served registry", name)
			continue
		}
		if len(got) != len(labels) {
			t.Errorf("%s labels = %v, want %v", name, got, labels)
			continue
		}
		for i := range labels {
			if got[i] != labels[i] {
				t.Errorf("%s labels = %v, want %v", name, got, labels)
				break
			}
		}
	}
	for name := range families {
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected metric family %s on the registry (contract is pinned)", name)
		}
	}
}

// The service label is the constant "notification" everywhere it appears.
func TestServiceLabelIsConstantNotification(t *testing.T) {
	for range gatherFamilies(t) { // ensure children exist
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		for _, m := range mf.Metric {
			for _, l := range m.Label {
				if l.GetName() == "service" && l.GetValue() != ServiceName {
					t.Errorf("%s service label = %q, want constant %q", mf.GetName(), l.GetValue(), ServiceName)
				}
			}
		}
	}
}

// Outcome label values are pinned: processed | absorbed.
func TestOutcomeLabelValuesArePinned(t *testing.T) {
	if OutcomeProcessed != "processed" || OutcomeAbsorbed != "absorbed" {
		t.Errorf("outcome constants = %q/%q, want processed/absorbed", OutcomeProcessed, OutcomeAbsorbed)
	}
}

// Handler serves the pinned registry over HTTP with 200 (httpapi mounts it on
// the EXISTING mux; endpoint shape asserted in the httpapi package tests).
func TestHandlerServesRegistry(t *testing.T) {
	for range gatherFamilies(t) { // ensure children exist
	}
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	for _, name := range []string{
		"orderflow_events_total",
		"orderflow_retries_total",
		"orderflow_dlq_parked_total",
		"orderflow_outbox_pending",
		"orderflow_outbox_published_total",
		"orderflow_notifications_published_total",
	} {
		if !strings.Contains(string(body), name) {
			t.Errorf("/metrics body missing %s", name)
		}
	}
}
