package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/notification/internal/metrics"
	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

// OutboxStore is the relay's persistence port (postgres.Store implements it).
type OutboxStore interface {
	// CountUnpublished returns the pending row count (published_at IS NULL);
	// refreshed into orderflow_outbox_pending on every relay tick.
	CountUnpublished(ctx context.Context) (int, error)
	FetchUnpublished(ctx context.Context, limit int) ([]notification.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventIDs []string, at time.Time) error
}

// Relay publishes committed outbox events to Kafka. Delivery contract
// (topology §7): idempotent producer with acks=all; a single ordered fetch +
// per-event synchronous produce preserves per-aggregate order; an event is
// marked published only after its ack, so failures retry in order.
type Relay struct {
	client   *kgo.Client
	store    OutboxStore
	serde    *Serde
	topic    string
	interval time.Duration
	// publish sends one event to the wire (schema id + encode + ProduceSync);
	// a seam so tests drive flush with fakes instead of a live broker.
	publish func(ctx context.Context, e notification.OutboxEvent) error
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
		client:   cl,
		store:    store,
		serde:    serde,
		topic:    topic,
		interval: 250 * time.Millisecond,
		publish:  publishWith(cl, serde, topic),
	}, nil
}

// publishWith binds the topic, serde and client into the wire publish step.
func publishWith(client *kgo.Client, serde *Serde, topic string) func(context.Context, notification.OutboxEvent) error {
	return func(ctx context.Context, e notification.OutboxEvent) error {
		id, err := serde.SchemaID(e.RecordType)
		if err != nil {
			return err
		}
		rec := &kgo.Record{
			Topic: topic,
			Key:   []byte(e.AggregateID),
			Value: serde.Encode(id, e.Payload),
			Headers: []kgo.RecordHeader{
				{Key: "event_type", Value: []byte(e.RecordType)},
				{Key: "event_id", Value: []byte(e.EventID)},
			},
		}
		results := client.ProduceSync(ctx, rec)
		for _, res := range results {
			if res.Err != nil {
				return fmt.Errorf("produce %s: %w", e.EventID, res.Err)
			}
		}
		return nil
	}
}

// Run polls until ctx is cancelled. Each tick first refreshes the pending
// gauge (orderflow_outbox_pending — parent of the L6 head-of-line alert),
// then flushes.
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.client.Close()
			return
		case <-t.C:
			r.tick(ctx)
		}
	}
}

// tick is one relay cycle: pending gauge refresh + flush attempt.
func (r *Relay) tick(ctx context.Context) {
	if n, err := r.store.CountUnpublished(ctx); err != nil {
		log.Printf("outbox relay: count pending: %v", err)
	} else {
		metrics.SetOutboxPending(float64(n))
	}
	if err := r.flush(ctx); err != nil {
		log.Printf("outbox relay: %v", err)
	}
}

func (r *Relay) flush(ctx context.Context) error {
	events, err := r.store.FetchUnpublished(ctx, 100)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	if len(events) == 0 {
		return nil
	}
	published := make([]string, 0, len(events))
	for _, e := range events {
		if err := r.publish(ctx, e); err != nil {
			return err
		}
		published = append(published, e.EventID)
	}
	if err := r.store.MarkPublished(ctx, published, time.Now().UTC()); err != nil {
		return err
	}
	metrics.AddOutboxPublished(r.topic, len(published))
	return nil
}
