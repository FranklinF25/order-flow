package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/inventory/internal/events"
	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
)

// Consumer consumes order.events in group inventory.order-events (topology §2)
// and reacts ONLY to orderflow.order.OrderPlaced (topology §6 subscriptions).
//
// Commit strategy (topology §7 at-least-once): auto-commit is disabled and
// each record's offset is committed explicitly AFTER its handler transaction
// commits. A crash between handler commit and offset commit replays the
// record; the processed_events insert inside the same handler transaction
// makes the replay a silent no-op. M1 has no retries/DLQ (topology §7): on a
// handler error the consumer stops uncommitted, so the record is redelivered
// on the next start.
type Consumer struct {
	client  *kgo.Client
	svc     *inventory.Service
	handler Handler // c.handle wrapped with the Redis fast-gate (nil gate → unchanged)
}

// NewConsumer builds the order.events consumer. gate may be nil (REDIS_URL
// unset): the fast-gate is disabled and records dispatch exactly as before.
func NewConsumer(brokers []string, topic, group string, svc *inventory.Service, gate FastGate) (*Consumer, error) {
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
	c := &Consumer{client: cl, svc: svc}
	// Fast-gate before dispatch (topology §7 first gate); absorb-on-hit
	// returns nil, so the offset commits exactly like the duplicate path.
	c.handler = WithFastGate(gate, c.handle)
	return c, nil
}

// Run polls until ctx is cancelled or a handler fails (no retries in M1).
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
				log.Printf("consumer stopped on %s[%d] offset %d: %v (M1: no retry/DLQ)", rec.Topic, rec.Partition, rec.Offset, err)
				c.client.Close()
				return
			}
			if err := c.client.CommitRecords(ctx, rec); err != nil {
				if ctx.Err() == nil {
					log.Printf("commit %s[%d] offset %d failed: %v (M1: no retry/DLQ)", rec.Topic, rec.Partition, rec.Offset, err)
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
	if headerValue(rec, "event_type") != events.OrderPlacedType {
		return nil // OrderConfirmed / OrderCancelled never reach the domain
	}
	payload, err := StripWire(rec.Value)
	if err != nil {
		return err
	}
	ev, err := events.DecodeOrderPlaced(payload)
	if err != nil {
		return fmt.Errorf("decode OrderPlaced: %w", err)
	}
	items := make([]inventory.Item, len(ev.Items))
	for i, it := range ev.Items {
		items[i] = inventory.Item{SKU: it.SKU, Qty: it.Qty}
	}
	_, err = c.svc.HandleOrderPlaced(ctx, inventory.ReserveInput{
		EventID:    ev.EventID,
		OrderID:    ev.OrderID,
		CustomerID: ev.CustomerID, // v3 echo: payment evaluates its marker downstream
		TotalCents: ev.TotalCents,
		Items:      items,
	})
	return err
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
