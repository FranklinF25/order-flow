package notification

import (
	"context"
	"errors"
	"testing"
)

// fakeStore is an in-memory Store for deterministic app-service tests.
type fakeStore struct {
	notifications []Notification
	outbox        []OutboxEvent
	processed     map[string]bool
	failOn        string // method name to force an error
}

func newFakeStore() *fakeStore {
	return &fakeStore{processed: map[string]bool{}}
}

func (f *fakeStore) SaveNotification(_ context.Context, n *Notification) error {
	if f.failOn == "SaveNotification" {
		return errors.New("boom")
	}
	f.notifications = append(f.notifications, *n)
	return nil
}

func (f *fakeStore) AppendOutbox(_ context.Context, e OutboxEvent) error {
	if f.failOn == "AppendOutbox" {
		return errors.New("boom")
	}
	f.outbox = append(f.outbox, e)
	return nil
}

func (f *fakeStore) InsertProcessedEvent(_ context.Context, eventID string) (bool, error) {
	if f.failOn == "InsertProcessedEvent" {
		return false, errors.New("boom")
	}
	if f.processed[eventID] {
		return false, nil
	}
	f.processed[eventID] = true
	return true, nil
}

// fakeTx models a real DB transaction: rollback restores prior state on error.
type fakeTx struct{ store *fakeStore }

func (f *fakeTx) WithinTx(ctx context.Context, fn func(context.Context, Store) error) error {
	notifSnap := append([]Notification(nil), f.store.notifications...)
	outboxSnap := append([]OutboxEvent(nil), f.store.outbox...)
	processedSnap := make(map[string]bool, len(f.store.processed))
	for k, v := range f.store.processed {
		processedSnap[k] = v
	}
	if err := fn(ctx, f.store); err != nil {
		f.store.notifications = notifSnap
		f.store.outbox = outboxSnap
		f.store.processed = processedSnap
		return err
	}
	return nil
}

// fakePublisher records publish intents and can simulate a broker outage.
type fakePublisher struct {
	published []Notification
	fail      bool
}

func (f *fakePublisher) Publish(_ context.Context, n Notification) error {
	if f.fail {
		return errors.New("rabbitmq down")
	}
	f.published = append(f.published, n)
	return nil
}

// fakeEncoder stands in for the Avro driven adapter.
type fakeEncoder struct{}

func (fakeEncoder) NotificationSent(n Notification) (OutboxEvent, error) {
	return OutboxEvent{
		EventID:     "evt-ns-" + n.ID,
		RecordType:  "orderflow.notification.NotificationSent",
		AggregateID: n.OrderID,
		Payload:     []byte("avro-bytes"),
	}, nil
}

// Rule 1: one transaction persists processed_events + the notifications audit
// row (order_id, kind, channel EMAIL) + the NotificationSent outbox event;
// after commit the publish intent goes out with the routing key matching the
// kind (topology §8: order.confirmed / order.cancelled).
func TestNotifyPersistsRowsAndPublishesIntent(t *testing.T) {
	store := newFakeStore()
	pub := &fakePublisher{}
	svc := NewService(&fakeTx{store}, pub, fakeEncoder{})

	if _, err := svc.Notify(context.Background(), NotifyInput{EventID: "evt-oc-1", OrderID: "ord-1", Kind: KindOrderConfirmed}); err != nil {
		t.Fatalf("Notify(confirmed): %v", err)
	}
	if !store.processed["evt-oc-1"] {
		t.Error("processed_events guard not persisted in the same tx")
	}
	if len(store.notifications) != 1 {
		t.Fatalf("notification rows = %d, want 1", len(store.notifications))
	}
	n := store.notifications[0]
	if n.OrderID != "ord-1" || n.Kind != KindOrderConfirmed || n.Channel != ChannelEmail {
		t.Errorf("audit row = %+v, want ord-1/ORDER_CONFIRMED/EMAIL", n)
	}
	if n.ID == "" {
		t.Error("notification id must be generated")
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1 (NotificationSent)", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.RecordType != "orderflow.notification.NotificationSent" || ev.AggregateID != "ord-1" {
		t.Errorf("outbox event = %+v, want NotificationSent keyed by order id", ev)
	}
	if ev.EventID == "" || len(ev.Payload) == 0 {
		t.Error("outbox event must carry an event id and an encoded payload")
	}
	if len(pub.published) != 1 {
		t.Fatalf("publish intents = %d, want 1", len(pub.published))
	}
	if got := pub.published[0]; got.OrderID != "ord-1" || got.Kind != KindOrderConfirmed {
		t.Errorf("publish intent = %+v, want ord-1/ORDER_CONFIRMED", got)
	}
	if got := pub.published[0].Kind.RoutingKey(); got != "order.confirmed" {
		t.Errorf("routing key = %q, want order.confirmed", got)
	}

	// Same contract for OrderCancelled with its own routing key (rule 1, second kind).
	if _, err := svc.Notify(context.Background(), NotifyInput{EventID: "evt-oc-2", OrderID: "ord-2", Kind: KindOrderCancelled}); err != nil {
		t.Fatalf("Notify(cancelled): %v", err)
	}
	if got := store.notifications[1]; got.Kind != KindOrderCancelled || got.Channel != ChannelEmail {
		t.Errorf("audit row = %+v, want ORDER_CANCELLED/EMAIL", got)
	}
	if got := pub.published[1].Kind.RoutingKey(); got != "order.cancelled" {
		t.Errorf("routing key = %q, want order.cancelled", got)
	}
}

// Triangulation (topology §8): a RabbitMQ failure after commit is logged and
// swallowed — the tx rows stay committed and Notify returns nil so the offset
// commits safely. The Kafka outbox is the audit of record.
func TestNotifyRabbitFailureIsBestEffort(t *testing.T) {
	store := newFakeStore()
	pub := &fakePublisher{fail: true}
	svc := NewService(&fakeTx{store}, pub, fakeEncoder{})

	if _, err := svc.Notify(context.Background(), NotifyInput{EventID: "evt-oc-1", OrderID: "ord-1", Kind: KindOrderConfirmed}); err != nil {
		t.Fatalf("Notify must swallow rabbit failures, got %v", err)
	}
	if !store.processed["evt-oc-1"] || len(store.notifications) != 1 || len(store.outbox) != 1 {
		t.Errorf("tx rows must survive a rabbit outage (processed=%v rows=%d outbox=%d)",
			store.processed["evt-oc-1"], len(store.notifications), len(store.outbox))
	}
	if len(pub.published) != 0 {
		t.Errorf("failed publishes must not be recorded as delivered, got %d", len(pub.published))
	}
}

// Triangulation: an invalid kind is rejected before any tx touches the store.
func TestNotifyRejectsInvalidKind(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, &fakePublisher{}, fakeEncoder{})

	fresh, err := svc.Notify(context.Background(), NotifyInput{EventID: "evt-x", OrderID: "ord-1", Kind: Kind("SURPRISE")})
	if !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("err = %v, want ErrInvalidKind", err)
	}
	if fresh {
		t.Error("rejected input must not report fresh")
	}
	if len(store.notifications) != 0 || len(store.outbox) != 0 || len(store.processed) != 0 {
		t.Error("invalid kind must persist nothing")
	}
}

// Rule 2: duplicate event ids are absorbed by processed_events — no second
// audit row, no second outbox fact, no second publish intent, and the handler
// returns nil so the offset commits safely.
func TestNotifyDuplicateEventIsAbsorbed(t *testing.T) {
	store := newFakeStore()
	pub := &fakePublisher{}
	svc := NewService(&fakeTx{store}, pub, fakeEncoder{})
	in := NotifyInput{EventID: "evt-oc-1", OrderID: "ord-1", Kind: KindOrderConfirmed}

	fresh, err := svc.Notify(context.Background(), in)
	if err != nil || !fresh {
		t.Fatalf("first Notify: err=%v fresh=%v, want nil/true", err, fresh)
	}
	fresh, err = svc.Notify(context.Background(), in)
	if err != nil {
		t.Fatalf("duplicate replay must be a no-op, got %v", err)
	}
	if fresh {
		t.Error("duplicate replay must report fresh=false (absorbed)")
	}
	if len(store.notifications) != 1 {
		t.Errorf("notification rows = %d, want 1 after replay", len(store.notifications))
	}
	if len(store.outbox) != 1 {
		t.Errorf("outbox events = %d, want 1 after replay", len(store.outbox))
	}
	if len(pub.published) != 1 {
		t.Errorf("publish intents = %d, want 1 after replay", len(pub.published))
	}
}
