package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/inventory/internal/events"
	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
	"github.com/FranklinF25/order-flow/services/inventory/internal/metrics"
)

// Consumer consumes order.events in group inventory.order-events (topology §2)
// and reacts ONLY to orderflow.order.OrderPlaced (topology §6 subscriptions).
//
// Commit strategy (topology §7 at-least-once): auto-commit is disabled and
// each record's offset is committed explicitly AFTER its handler transaction
// commits. A crash between handler commit and offset commit replays the
// record; the processed_events insert inside the same handler transaction
// makes the replay a silent no-op. Retries/DLQ (topology §7) wrap the
// handler inside the fast-gate: 3 blocking attempts, then the raw record
// parks on <topic>.<group>.dlq and the offset commits; the only error that
// reaches Run is a DLQ-park failure, which stops the consumer uncommitted.
type Consumer struct {
	client  *kgo.Client
	svc     *inventory.Service
	group   string // consumer group — the pinned orderflow_events_total group label
	handler Handler // c.handle wrapped with the Redis fast-gate (nil gate → unchanged)
}

// NewConsumer builds the order.events consumer. gate may be nil (REDIS_URL
// unset): the fast-gate is disabled and records dispatch exactly as before.
// dlq carries the retries/DLQ wrapper (topology §7): composition outside-in
// is fast-gate → retries/DLQ → handle, so a gate absorb never burns a retry.
func NewConsumer(brokers []string, topic, group string, svc *inventory.Service, gate FastGate, dlq DLQProducer) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// auto.offset.reset=earliest: a brand-new group starts at the oldest
		// retained offset (topology §7).
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// offsets are committed per handled record, after its tx commits
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	c := &Consumer{client: cl, svc: svc, group: group}
	// Fast-gate before dispatch (topology §7 first gate); absorb-on-hit
	// returns nil, so the offset commits exactly like the duplicate path.
	// Retries/DLQ inside the gate: 3 blocking attempts, then the raw record
	// is parked on <topic>.<group>.dlq and the offset commits.
	c.handler = WithFastGate(gate, WithRetries(dlq, RetryBaseDelay, group, c.handle))
	return c, nil
}

// Run polls until ctx is cancelled or the handler fails. The handler already
// applied retries + DLQ (topology §7), so the only error reaching Run is a
// DLQ-park failure: stop uncommitted so the record redelivers and re-runs
// the whole path (at-least-once into the DLQ).
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.client.PollRecords(ctx, 50)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("consumer fetch error %s[%d]: %v", topic, partition, err)
		})
		for _, rec := range fetches.Records() {
			if ctx.Err() != nil {
				break
			}
			if err := c.handler(ctx, rec); err != nil {
				log.Printf("consumer stopped on %s[%d] offset %d: %v (DLQ park failed; redelivery re-runs the path)", rec.Topic, rec.Partition, rec.Offset, err)
				c.client.Close()
				return
			}
			if err := c.client.CommitRecords(ctx, rec); err != nil {
				if ctx.Err() == nil {
					log.Printf("commit %s[%d] offset %d failed: %v (stopping; record redelivers)", rec.Topic, rec.Partition, rec.Offset, err)
					c.client.Close()
					return
				}
			}
		}
	}
	c.client.Close()
}

// handle decodes one record and reserves stock for OrderPlaced facts. Any
// other event_type on the stream is not inventory's concern and is skipped.
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	eventType := headerValue(rec, "event_type")
	if eventType != events.OrderPlacedType {
		// Pass-through: OrderConfirmed / OrderCancelled never reach the domain.
		// Pinned contract: non-matching event_types classify as absorbed.
		metrics.IncEvent(c.group, rec.Topic, eventType, metrics.OutcomeAbsorbed)
		return nil
	}
	payload, err := StripWire(rec.Value)
	if err != nil {
		return err // no outcome yet: the retry/DLQ counters own this record
	}
	ev, err := events.DecodeOrderPlaced(payload)
	if err != nil {
		return fmt.Errorf("decode OrderPlaced: %w", err)
	}
	items := make([]inventory.Item, len(ev.Items))
	for i, it := range ev.Items {
		items[i] = inventory.Item{SKU: it.SKU, Qty: it.Qty}
	}
	applied, err := c.svc.HandleOrderPlaced(ctx, inventory.ReserveInput{
		EventID:    ev.EventID,
		OrderID:    ev.OrderID,
		CustomerID: ev.CustomerID, // v3 echo: payment evaluates its marker downstream
		TotalCents: ev.TotalCents,
		Items:      items,
	})
	if err != nil {
		return err // no outcome yet: the retry/DLQ counters own this record
	}
	metrics.IncEvent(c.group, rec.Topic, eventType, outcomeLabel(applied))
	return nil
}

// outcomeLabel maps the domain's applied flag to the pinned outcome
// vocabulary: a committed fact is processed; a duplicate, unknown order or
// already-RELEASED compensation is absorbed.
func outcomeLabel(applied bool) string {
	if applied {
		return metrics.OutcomeProcessed
	}
	return metrics.OutcomeAbsorbed
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
