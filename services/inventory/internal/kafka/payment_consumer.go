package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/inventory/internal/events"
	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
)

// PaymentConsumer consumes payment.events in group inventory.payment-events
// (topology §2) and reacts ONLY to orderflow.payment.PaymentFailed — the
// compensation B trigger (M2): release the reservation, restore stock and emit
// StockReleased via the same one-transaction application service the
// order.events consumer uses.
//
// Commit strategy (topology §7 at-least-once, same posture as Consumer):
// auto-commit is disabled and each record's offset is committed explicitly
// AFTER its handler transaction commits. A crash between handler commit and
// offset commit replays the record; the processed_events insert and the
// reservation state machine make the replay an absorbed no-op. M2 has no
// retries/DLQ yet: on a handler error the consumer stops uncommitted, so the
// record is redelivered on the next start.
type PaymentConsumer struct {
	client  *kgo.Client
	svc     *inventory.Service
	handler Handler // c.handle wrapped with the Redis fast-gate (nil gate → unchanged)
}

// NewPaymentConsumer builds the payment.events consumer. gate may be nil
// (REDIS_URL unset): the fast-gate is disabled and records dispatch as before.
func NewPaymentConsumer(brokers []string, topic, group string, svc *inventory.Service, gate FastGate) (*PaymentConsumer, error) {
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
	c := &PaymentConsumer{client: cl, svc: svc}
	// Fast-gate before dispatch (topology §7 first gate); absorb-on-hit
	// returns nil, so the offset commits exactly like the absorbed no-op path.
	c.handler = WithFastGate(gate, c.handle)
	return c, nil
}

// Run polls until ctx is cancelled or a handler fails (no retry/DLQ in M2 yet).
func (c *PaymentConsumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.client.PollRecords(ctx, 50)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("payment consumer fetch error %s[%d]: %v", topic, partition, err)
		})
		for _, rec := range fetches.Records() {
			if ctx.Err() != nil {
				break
			}
			if err := c.handler(ctx, rec); err != nil {
				log.Printf("payment consumer stopped on %s[%d] offset %d: %v (no retry/DLQ yet)", rec.Topic, rec.Partition, rec.Offset, err)
				c.client.Close()
				return
			}
			if err := c.client.CommitRecords(ctx, rec); err != nil {
				if ctx.Err() == nil {
					log.Printf("commit %s[%d] offset %d failed: %v (no retry/DLQ yet)", rec.Topic, rec.Partition, rec.Offset, err)
					c.client.Close()
					return
				}
			}
		}
	}
	c.client.Close()
}

// handle decodes one record and releases the order's reservation for
// PaymentFailed facts. Any other event_type on the stream is not inventory's
// concern and is skipped.
func (c *PaymentConsumer) handle(ctx context.Context, rec *kgo.Record) error {
	if headerValue(rec, "event_type") != events.PaymentFailedType {
		return nil // PaymentCaptured / future facts never reach the domain
	}
	payload, err := StripWire(rec.Value)
	if err != nil {
		return err
	}
	ev, err := events.DecodePaymentFailed(payload)
	if err != nil {
		return fmt.Errorf("decode PaymentFailed: %w", err)
	}
	applied, err := c.svc.HandlePaymentFailed(ctx, inventory.ReleaseInput{
		EventID: ev.EventID,
		OrderID: ev.OrderID,
	})
	if err != nil {
		return err
	}
	if !applied {
		// Absorbed: duplicate event, unknown order or already RELEASED. The
		// transaction committed (or was a no-op); the offset commits above.
		log.Printf("payment consumer: PaymentFailed %s for order %s absorbed (no-op)", ev.EventID, ev.OrderID)
	}
	return nil
}
