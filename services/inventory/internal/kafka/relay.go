package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
	"github.com/FranklinF25/order-flow/services/inventory/internal/metrics"
)

// OutboxStore is the relay's persistence port (postgres.Store implements it).
type OutboxStore interface {
	// CountUnpublished returns the outbox backlog (published_at IS NULL); the
	// relay refreshes orderflow_outbox_pending from it every tick (M3).
	CountUnpublished(ctx context.Context) (int64, error)
	FetchUnpublished(ctx context.Context, limit int) ([]inventory.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventIDs []string, at time.Time) error
}

// recordProducer is the relay's produce port: one synchronous, broker-acked
// record write. The franz-go adapter wraps ProduceSync; tests inject fakes
// (M3: this keeps the outbox metric paths provable without a broker).
type recordProducer interface {
	Produce(ctx context.Context, rec *kgo.Record) error
	Close()
}

// kgoProducer adapts a franz-go client to recordProducer. The first failed
// partition response of the synchronous produce is the returned error — the
// exact contract the flush loop enforced inline before M3.
type kgoProducer struct{ client *kgo.Client }

func (p kgoProducer) Produce(ctx context.Context, rec *kgo.Record) error {
	for _, res := range p.client.ProduceSync(ctx, rec) {
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

func (p kgoProducer) Close() { p.client.Close() }

// Relay publishes committed outbox events to Kafka. Delivery contract
// (topology §7): idempotent producer (franz-go default) with acks=all; a
// single ordered fetch + per-event synchronous produce preserves per-aggregate
// order; an event is marked published only after its ack, so failures retry in
// order on the next tick.
type Relay struct {
	producer recordProducer
	store    OutboxStore
	serde    *Serde
	topic    string
	interval time.Duration
}

// NewRelay builds the producer and relay.
func NewRelay(brokers []string, topic string, store OutboxStore, serde *Serde) (*Relay, error) {
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
		producer: kgoProducer{client: cl},
		store:    store,
		serde:    serde,
		topic:    topic,
		interval: 250 * time.Millisecond,
	}, nil
}

// Run polls until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.producer.Close()
			return
		case <-t.C:
			if err := r.flush(ctx); err != nil {
				log.Printf("outbox relay: %v", err)
			}
		}
	}
}

func (r *Relay) flush(ctx context.Context) error {
	// M3 contract: refresh orderflow_outbox_pending on EVERY tick — most
	// importantly while produces fail, because the head-of-line stall is
	// exactly when the gauge must show the backlog. A count failure is logged
	// and never blocks publishing.
	if n, err := r.store.CountUnpublished(ctx); err != nil {
		log.Printf("outbox relay: count pending: %v", err)
	} else {
		metrics.SetOutboxPending(n)
	}
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
		if err := r.producer.Produce(ctx, rec); err != nil {
			return fmt.Errorf("produce %s: %w", e.EventID, err)
		}
		// Broker-acked: the event counts as published even if the later
		// MarkPublished fails — redelivery then re-publishes (at-least-once).
		metrics.IncOutboxPublished(r.topic)
		published = append(published, e.EventID)
	}
	return r.store.MarkPublished(ctx, published, time.Now().UTC())
}
