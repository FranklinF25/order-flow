package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

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
	handler Handler // handleWithRetry wrapped with the Redis fast-gate (nil gate → unchanged)
}

// NewConsumer builds the inventory.events group consumer. gate may be nil
// (REDIS_URL unset): the fast-gate is disabled and records dispatch as before.
func NewConsumer(brokers []string, svc *payment.Service, dec *events.Decoder, gate FastGate) (*Consumer, error) {
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
	// Fast-gate outside the retry loop (topology §7 first gate): a hit
	// absorbs before any attempt and the offset commits; a check error
	// degrades into the normal retried path; Mark runs only after the final
	// success — the tx has committed by then (Capture's processed_events
	// guard is the real gate).
	c.handler = WithFastGate(gate, c.handleWithRetry)
	return c, nil
}

// Run polls until ctx is cancelled. One record per poll keeps handle→commit
// strictly sequential and per-order ordered inside the partition.
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
				// No commit: the group position stays behind, so the record
				// redelivers after restart/rebalance. M2 replaces this with
				// the DLQ per topology §7 (3 retries already applied here).
				log.Printf("%s: giving up on event %s: %v", consumerGroup,
					headerValue(rec, "event_id"), err)
				return
			}
			if err := c.client.CommitRecords(ctx, rec); err != nil && ctx.Err() == nil {
				log.Printf("%s: commit: %v", consumerGroup, err)
			}
		})
	}
}

// handleWithRetry applies §7's blocking in-consumer retries (3 attempts,
// exponential backoff) before giving up without committing.
func (c *Consumer) handleWithRetry(ctx context.Context, rec *kgo.Record) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(1<<(attempt-1)) * 250 * time.Millisecond):
			}
		}
		if err = c.handle(ctx, rec); err == nil {
			return nil
		}
		log.Printf("%s: attempt %d: %v", consumerGroup, attempt+1, err)
	}
	return err
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
