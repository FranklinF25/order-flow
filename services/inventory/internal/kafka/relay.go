package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
)

// OutboxStore is the relay's persistence port (postgres.Store implements it).
type OutboxStore interface {
	FetchUnpublished(ctx context.Context, limit int) ([]inventory.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventIDs []string, at time.Time) error
}

// Relay publishes committed outbox events to Kafka. Delivery contract
// (topology §7): idempotent producer (franz-go default) with acks=all; a
// single ordered fetch + per-event synchronous produce preserves per-aggregate
// order; an event is marked published only after its ack, so failures retry in
// order on the next tick.
type Relay struct {
	client   *kgo.Client
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
		client:   cl,
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
			r.client.Close()
			return
		case <-t.C:
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
		published = append(published, e.EventID)
	}
	return r.store.MarkPublished(ctx, published, time.Now().UTC())
}
