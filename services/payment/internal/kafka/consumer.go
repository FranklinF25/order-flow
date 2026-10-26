package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/payment/internal/events"
	"github.com/FranklinF25/order-flow/services/payment/internal/metrics"
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
	m       *metrics.Metrics
	handler Handler // handle wrapped with fast-gate → retries/DLQ (nil gate → gate passes through)
}

// NewConsumer builds the inventory.events group consumer. gate may be nil
// (REDIS_URL unset): the fast-gate is disabled and records dispatch as
// before. dlq carries the retries/DLQ wrapper (topology §7). m instruments
// the pinned M3 metric contract (nil disables counting). Composition is
// outside-in — fast-gate → retries/DLQ → handle — so a gate absorb returns
// before any retry attempt is burned, and the internal handleWithRetry loop
// is gone: exactly one retry mechanism, exactly 3 total attempts. The
// metrics ride the composition, not the wrappers: the DLQ producer is
// decorated for park counts and handle's input for blocked-attempt counts
// (internal/kafka/metrics.go), leaving WithFastGate/WithRetries untouched.
func NewConsumer(brokers []string, svc *payment.Service, dec *events.Decoder, gate FastGate, dlq DLQProducer, m *metrics.Metrics) (*Consumer, error) {
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
	c := &Consumer{client: cl, svc: svc, dec: dec, m: m}
	// Fast-gate outside the retries/DLQ wrapper (topology §7 first gate): a
	// hit absorbs before any attempt and the offset commits; a check error
	// degrades into the normal retried path; Mark runs only after the final
	// success — the tx has committed by then (Capture's processed_events
	// guard is the real gate). On a poison record the wrapper parks it on
	// <topic>.<group>.dlq and returns nil, so the offset commits below.
	// Metric decorators: park counts wrap the DLQ port, blocked-attempt
	// counts wrap the handler fed INTO the retry loop.
	c.handler = WithFastGate(gate, WithRetries(newMetricsDLQ(dlq, m, consumerGroup), RetryBaseDelay, consumerGroup,
		blockedAttemptHandler{next: c.handle, m: m, group: consumerGroup}.handle))
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
//
// Metric contract (M3): events_total is counted here, after the commit
// decision — errors return BEFORE counting so a redelivered record never
// double-counts. Non-matching event_type pass-throughs classify as absorbed;
// a fresh DB guard insert is processed; a duplicate-marker replay the DB
// absorbs (Capture fresh=false) is absorbed. Gate hits absorb one layer up
// and stay uncounted (documented deviation). A decode failure is also
// uncounted: the offset never commits, the retry loop re-runs the path.
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	eventType := headerValue(rec, "event_type")
	if eventType != events.StockReservedType {
		c.m.IncEvents(consumerGroup, rec.Topic, eventType, metrics.OutcomeAbsorbed)
		return nil
	}
	evt, err := c.dec.DecodeStockReserved(rec.Value)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	fresh, err := c.svc.Capture(ctx, payment.CaptureInput{
		EventID:     evt.EventID,
		OrderID:     evt.OrderID,
		AmountCents: evt.TotalCents, // the v2 charge amount (topology §5)
		CustomerID:  evt.CustomerID, // the v3 failure marker for the simulated gateway (M2, S3)
	})
	if err != nil {
		return err // uncommitted: no count, redelivery re-runs the whole path
	}
	outcome := metrics.OutcomeProcessed
	if !fresh {
		outcome = metrics.OutcomeAbsorbed // duplicate-marker replay the DB absorbed
	}
	c.m.IncEvents(consumerGroup, rec.Topic, eventType, outcome)
	return nil
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
