package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/payment/internal/metrics"
	"github.com/FranklinF25/order-flow/services/payment/internal/payment"
)

// OutboxStore is the relay's persistence port (postgres.Store implements it).
type OutboxStore interface {
	// CountUnpublished counts pending rows (published_at IS NULL) — the M3
	// orderflow_outbox_pending gauge refreshes from it on every relay tick.
	CountUnpublished(ctx context.Context) (int64, error)
	FetchUnpublished(ctx context.Context, limit int) ([]payment.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventIDs []string, at time.Time) error
}

// publisher is the produce port of the relay: the kgo client in production
// (named so flush is testable without a broker — unit tests inject a fake).
type publisher interface {
	PublishSync(ctx context.Context, rec *kgo.Record) error
}

// kgoPublisher adapts the franz-go client to the publisher port.
type kgoPublisher struct{ client *kgo.Client }

func (p kgoPublisher) PublishSync(ctx context.Context, rec *kgo.Record) error {
	for _, res := range p.client.ProduceSync(ctx, rec) {
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

// recordEncoder is the serde port of the relay (*Serde implements it).
type recordEncoder interface {
	SchemaID(recordType string) (int, error)
	Encode(schemaID int, payload []byte) []byte
}

// Relay publishes committed outbox events to Kafka. Delivery contract
// (topology §7): idempotent producer with acks=all; a single ordered fetch +
// per-event synchronous produce preserves per-aggregate order; an event is
// marked published only after its ack, so failures retry in order.
//
// M3 metrics (pinned contract): orderflow_outbox_pending refreshes from
// CountUnpublished on EVERY tick (even when nothing is ready), and
// orderflow_outbox_published_total counts each event right after its
// synchronous produce acks — a later MarkPublished failure leaves the count
// standing because the publish DID happen (at-least-once repeats it).
type Relay struct {
	pub      publisher
	store    OutboxStore
	serde    recordEncoder
	topic    string
	interval time.Duration
	m        *metrics.Metrics
}

// NewRelay builds the producer and relay.
func NewRelay(brokers []string, topic string, store OutboxStore, serde *Serde, m *metrics.Metrics) (*Relay, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// idempotent produce is franz-go's default (DisableIdempotentWrite opts out)
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Relay{
		pub:      kgoPublisher{client: cl},
		store:    store,
		serde:    serde,
		topic:    topic,
		interval: 250 * time.Millisecond,
		m:        m,
	}, nil
}

// Run polls until ctx is cancelled. On shutdown the producer closes before
// the relay stops (Run owns the client lifetime, independent from the DLQ
// producer's dedicated client).
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.close()
			return
		case <-t.C:
			if err := r.flush(ctx); err != nil {
				log.Printf("outbox relay: %v", err)
			}
		}
	}
}

func (r *Relay) close() {
	if k, ok := r.pub.(kgoPublisher); ok {
		k.client.Close()
	}
}

func (r *Relay) flush(ctx context.Context) error {
	// Gauge refresh comes FIRST so the pending depth is re-reported on every
	// tick, including the empty-queue one (0 must be observable — the L6
	// alert clears on it).
	pending, err := r.store.CountUnpublished(ctx)
	if err != nil {
		return fmt.Errorf("count unpublished: %w", err)
	}
	r.m.SetOutboxPending(pending)
	events, err := r.store.FetchUnpublished(ctx, 100)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	if len(events) == 0 {
		return nil
	}
	published := make([]string, 0, len(events))
	for _, e := range events {
		id, err := r.serde.SchemaID(e.RecordType)
		if err != nil {
			return err
		}
		rec := &kgo.Record{
			Topic: r.topic,
			Key:   []byte(e.AggregateID),
			Value: r.serde.Encode(id, e.Payload),
			Headers: []kgo.RecordHeader{
				{Key: "event_type", Value: []byte(e.RecordType)},
				{Key: "event_id", Value: []byte(e.EventID)},
			},
		}
		if err := r.pub.PublishSync(ctx, rec); err != nil {
			return fmt.Errorf("produce %s: %w", e.EventID, err)
		}
		r.m.IncOutboxPublished(r.topic) // acked: count it even if MarkPublished fails below
		published = append(published, e.EventID)
	}
	return r.store.MarkPublished(ctx, published, time.Now().UTC())
}
