package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/payment/internal/events"
	"github.com/FranklinF25/order-flow/services/payment/internal/payment"
)

const (
	// Topology §2: payment consumes inventory.events as group
	// payment.inventory-events; new groups start at the earliest retained
	// offset (§7).
	inventoryEventsTopic = "inventory.events"
	consumerGroup        = "payment.inventory-events"
)

// Consumer is the driving adapter that reacts to StockReserved facts.
type Consumer struct {
	client  *kgo.Client
	svc     *payment.Service
	dec     *events.Decoder
	handler Handler // handle wrapped with fast-gate → retries/DLQ (nil gate → gate passes through)
}

// NewConsumer builds the inventory.events group consumer. gate may be nil
// (REDIS_URL unset): the fast-gate is disabled and records dispatch as
// before. dlq carries the retries/DLQ wrapper (topology §7). Composition is
// outside-in — fast-gate → retries/DLQ → handle — so a gate absorb returns
// before any retry attempt is burned, and the internal handleWithRetry loop
// is gone: exactly one retry mechanism, exactly 3 total attempts.
func NewConsumer(brokers []string, svc *payment.Service, dec *events.Decoder, gate FastGate, dlq DLQProducer) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(inventoryEventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // auto.offset.reset=earliest
		// Topology §7 at-least-once: offsets commit only AFTER the local
		// transaction commits, so a crash replays instead of losing the fact.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	c := &Consumer{client: cl, svc: svc, dec: dec}
	// Fast-gate outside the retries/DLQ wrapper (topology §7 first gate): a
	// hit absorbs before any attempt and the offset commits; a check error
	// degrades into the normal retried path; Mark runs only after the final
	// success — the tx has committed by then (Capture's processed_events
	// guard is the real gate). On a poison record the wrapper parks it on
	// <topic>.<group>.dlq and returns nil, so the offset commits below.
	c.handler = WithFastGate(gate, WithRetries(dlq, RetryBaseDelay, consumerGroup, c.handle))
	return c, nil
}

// Run polls until ctx is cancelled. One record per poll keeps handle→commit
// strictly sequential and per-order ordered inside the partition. The
// handler already applied retries + DLQ (topology §7), so the only error
// reaching Run is a DLQ-park failure: stop uncommitted so the record
// redelivers and re-runs the whole path (at-least-once into the DLQ).
func (c *Consumer) Run(ctx context.Context) {
	defer c.client.Close()
	for {
		fetches := c.client.PollRecords(ctx, 1)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("%s consumer: %v", consumerGroup, err)
			}
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			if err := c.handler(ctx, rec); err != nil {
				log.Printf("%s: stopping uncommitted on %s[%d] offset %d (DLQ park failed; redelivery re-runs the path): %v",
					consumerGroup, rec.Topic, rec.Partition, rec.Offset, err)
				c.client.Close()
				return
			}
			if err := c.client.CommitRecords(ctx, rec); err != nil && ctx.Err() == nil {
				log.Printf("%s: commit: %v", consumerGroup, err)
			}
		})
	}
}

// handle reacts ONLY to orderflow.inventory.StockReserved (topology §6);
// any other record on the stream is committed and skipped.
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	if headerValue(rec, "event_type") != events.StockReservedType {
		return nil
	}
	evt, err := c.dec.DecodeStockReserved(rec.Value)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return c.svc.Capture(ctx, payment.CaptureInput{
		EventID:     evt.EventID,
		OrderID:     evt.OrderID,
		AmountCents: evt.TotalCents, // the v2 charge amount (topology §5)
		CustomerID:  evt.CustomerID, // the v3 failure marker for the simulated gateway (M2, S3)
	})
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
