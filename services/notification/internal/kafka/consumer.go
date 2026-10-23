package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/notification/internal/events"
	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

const (
	// Topology §2/§8: notification consumes order.events as group
	// notification.order-events; new groups start at the earliest retained
	// offset (§7).
	orderEventsTopic = "order.events"
	consumerGroup    = "notification.order-events"
)

// Consumer is the driving adapter that reacts to OrderConfirmed and
// OrderCancelled facts.
type Consumer struct {
	client *kgo.Client
	svc    *notification.Service
	dec    *events.Decoder
}

// NewConsumer builds the order.events group consumer.
func NewConsumer(brokers []string, svc *notification.Service, dec *events.Decoder) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(orderEventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // auto.offset.reset=earliest
		// Topology §7 at-least-once: offsets commit only AFTER the local
		// transaction commits, so a crash replays instead of losing the fact.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Consumer{client: cl, svc: svc, dec: dec}, nil
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
			if err := c.handleWithRetry(ctx, rec); err != nil {
				// No commit: the group position stays behind, so the record
				// redelivers after restart/rebalance. The retries/DLQ wrapper
				// (topology §7) lands with the M2 inline pass.
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

// handle reacts ONLY to orderflow.order.OrderConfirmed and OrderCancelled
// (topology §5 subscriptions); any other record on the stream is committed
// and skipped.
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	switch headerValue(rec, "event_type") {
	case events.OrderConfirmedType:
		ev, err := c.dec.DecodeOrderConfirmed(rec.Value)
		if err != nil {
			return fmt.Errorf("decode OrderConfirmed: %w", err)
		}
		return c.svc.Notify(ctx, notification.NotifyInput{
			EventID: ev.EventID,
			OrderID: ev.OrderID,
			Kind:    notification.KindOrderConfirmed,
		})
	case events.OrderCancelledType:
		ev, err := c.dec.DecodeOrderCancelled(rec.Value)
		if err != nil {
			return fmt.Errorf("decode OrderCancelled: %w", err)
		}
		return c.svc.Notify(ctx, notification.NotifyInput{
			EventID: ev.EventID,
			OrderID: ev.OrderID,
			Kind:    notification.KindOrderCancelled,
		})
	}
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
