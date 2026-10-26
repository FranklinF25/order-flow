package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/order/internal/metrics"
	"github.com/FranklinF25/order-flow/services/order/internal/order"
)

// OutboxStore is the relay's persistence port (postgres.Store implements it).
type OutboxStore interface {
	FetchUnpublished(ctx context.Context, limit int) ([]order.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventIDs []string, at time.Time) error
	// CountUnpublished backs the pinned orderflow_outbox_pending gauge:
	// COUNT(*) WHERE published_at IS NULL, refreshed on every relay tick.
	CountUnpublished(ctx context.Context) (int64, error)
}

// schemaIDer is the serde seam of the relay (Serde implements it); an
// interface so tests can drive flush without a Schema Registry.
type schemaIDer interface {
	SchemaID(recordType string) (int, error)
	Encode(schemaID int, payload []byte) []byte
}

// outboxProducer is the produce seam of the relay (*kgo.Client implements
// it); an interface so tests can drive the publish path without a broker.
type outboxProducer interface {
	ProduceSync(ctx context.Context, recs ...*kgo.Record) kgo.ProduceResults
	Close()
}

// Relay publishes committed outbox events to Kafka. Delivery contract
// (topology §7): idempotent producer with acks=all; a single ordered fetch +
// per-event synchronous produce preserves per-aggregate order; an event is
// marked published only after its ack, so failures retry in order.
type Relay struct {
	client   outboxProducer
	store    OutboxStore
	serde    schemaIDer
	topic    string
	interval time.Duration
	metrics  *metrics.Metrics
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
		client:   cl,
		store:    store,
		serde:    serde,
		topic:    topic,
		interval: 250 * time.Millisecond,
		metrics:  m,
	}, nil
}

// Run polls until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.client.Close()
			return
		case <-t.C:
			if r.metrics != nil {
				// L6 head-of-line alert parent: the pending gauge refreshes on
				// EVERY tick, even when flush has nothing to publish.
				if n, err := r.store.CountUnpublished(ctx); err != nil {
					log.Printf("outbox relay: count pending: %v", err)
				} else {
					r.metrics.SetOutboxPending(float64(n))
				}
			}
			if err := r.flush(ctx); err != nil {
				log.Printf("outbox relay: %v", err)
			}
		}
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
		results := r.client.ProduceSync(ctx, rec)
		for _, res := range results {
			if res.Err != nil {
				return fmt.Errorf("produce %s: %w", e.EventID, res.Err)
			}
		}
		if r.metrics != nil {
			r.metrics.IncOutboxPublished(r.topic) // produce acked (acks=all)
		}
		published = append(published, e.EventID)
	}
	return r.store.MarkPublished(ctx, published, time.Now().UTC())
}
