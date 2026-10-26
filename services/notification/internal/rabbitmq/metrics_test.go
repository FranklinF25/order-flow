package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/FranklinF25/order-flow/services/notification/internal/metrics"
	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

// stubPublisher records intents and can simulate a broker outage.
type stubPublisher struct {
	calls int
	fail  bool
}

func (s *stubPublisher) Publish(_ context.Context, _ notification.Notification) error {
	s.calls++
	if s.fail {
		return errors.New("rabbitmq down")
	}
	return nil
}

func confirmed(t *testing.T) notification.Notification {
	t.Helper()
	n, err := notification.NewNotification("nid-1", "ord-1", notification.KindOrderConfirmed)
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}
	return n
}

func cancelled(t *testing.T) notification.Notification {
	t.Helper()
	n, err := notification.NewNotification("nid-2", "ord-2", notification.KindOrderCancelled)
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}
	return n
}

// A successful RabbitMQ publish increments orderflow_notifications_published_total
// exactly once, labeled by the routing key of the kind (order.confirmed /
// order.cancelled — topology §8).
func TestMetricsPublisherCountsByRoutingKeyKind(t *testing.T) {
	stub := &stubPublisher{}
	pub := NewMetricsPublisher(stub)
	ctx := context.Background()

	confirmedBefore := testutil.ToFloat64(metrics.NotificationsPublishedTotal.WithLabelValues(notification.RoutingKeyOrderConfirmed))
	cancelledBefore := testutil.ToFloat64(metrics.NotificationsPublishedTotal.WithLabelValues(notification.RoutingKeyOrderCancelled))

	if err := pub.Publish(ctx, confirmed(t)); err != nil {
		t.Fatalf("Publish(confirmed): %v", err)
	}
	if err := pub.Publish(ctx, cancelled(t)); err != nil {
		t.Fatalf("Publish(cancelled): %v", err)
	}

	if got := testutil.ToFloat64(metrics.NotificationsPublishedTotal.WithLabelValues(notification.RoutingKeyOrderConfirmed)) - confirmedBefore; got != 1 {
		t.Errorf("notifications_published_total{order.confirmed} delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.NotificationsPublishedTotal.WithLabelValues(notification.RoutingKeyOrderCancelled)) - cancelledBefore; got != 1 {
		t.Errorf("notifications_published_total{order.cancelled} delta = %v, want 1", got)
	}
}

// A failed publish counts nothing: the counter reports broker-accepted
// publishes only (best-effort drops are audited via the Kafka outbox).
func TestMetricsPublisherFailureCountsNothing(t *testing.T) {
	stub := &stubPublisher{fail: true}
	pub := NewMetricsPublisher(stub)

	before := testutil.ToFloat64(metrics.NotificationsPublishedTotal.WithLabelValues(notification.RoutingKeyOrderConfirmed))

	if err := pub.Publish(context.Background(), confirmed(t)); err == nil {
		t.Fatal("stub outage must surface as an error")
	}

	if got := testutil.ToFloat64(metrics.NotificationsPublishedTotal.WithLabelValues(notification.RoutingKeyOrderConfirmed)) - before; got != 0 {
		t.Errorf("notifications_published_total delta on failure = %v, want 0", got)
	}
	if stub.calls != 1 {
		t.Errorf("inner publish calls = %d, want 1 (delegation unchanged)", stub.calls)
	}
}
