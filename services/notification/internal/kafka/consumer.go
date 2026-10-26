package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/notification/internal/events"
	"github.com/FranklinF25/order-flow/services/notification/internal/metrics"
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
	client  *kgo.Client
	svc     *notification.Service
	dec     *events.Decoder
	handler Handler // handle wrapped with the retries/DLQ wrapper (topology §7)
}

// NewConsumer builds the order.events group consumer. dlq carries the
// retries/DLQ wrapper (topology §7), which replaces the old internal
// give-up-uncommitted retry loop: 3 blocking attempts, then the raw record
// parks on <topic>.<group>.dlq and the offset commits. Notification keeps no
// Redis gate in M2 (gate scope stays the 3 saga services); the processed_events
// guard inside Notify remains the idempotency backstop.
func NewConsumer(brokers []string, svc *notification.Service, dec *events.Decoder, dlq DLQProducer) (*Consumer, error) {
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
	c := &Consumer{client: cl, svc: svc, dec: dec}
	c.handler = WithRetries(dlq, RetryBaseDelay, consumerGroup, c.handle)
	return c, nil
}

// Run polls until ctx is cancelled. One record per poll keeps handle→commit
// strictly sequential and per-order ordered inside the partition. The handler
// already applied retries + DLQ (topology §7), so the only error reaching Run
// is a DLQ-park failure: stop uncommitted so the record redelivers and
// re-runs the whole path (at-least-once into the DLQ).
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

// handle reacts ONLY to orderflow.order.OrderConfirmed and OrderCancelled
// (topology §5 subscriptions); any other record on the stream is committed
// and skipped. Every handled record increments orderflow_events_total with
// the pinned outcome label: processed when the business effect applied,
// absorbed for duplicate replays and non-matching pass-throughs (DLQ parks
// are counted as absorbed by the WithRetries wrapper, which owns that
// offset-commit decision).
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	eventType := headerValue(rec, "event_type")
	switch eventType {
	case events.OrderConfirmedType:
		ev, err := c.dec.DecodeOrderConfirmed(rec.Value)
		if err != nil {
			return fmt.Errorf("decode OrderConfirmed: %w", err)
		}
		fresh, err := c.svc.Notify(ctx, notification.NotifyInput{
			EventID: ev.EventID,
			OrderID: ev.OrderID,
			Kind:    notification.KindOrderConfirmed,
		})
		if err != nil {
			return err
		}
		metrics.IncEvents(consumerGroup, rec.Topic, eventType, outcomeOf(fresh))
		return nil
	case events.OrderCancelledType:
		ev, err := c.dec.DecodeOrderCancelled(rec.Value)
		if err != nil {
			return fmt.Errorf("decode OrderCancelled: %w", err)
		}
		fresh, err := c.svc.Notify(ctx, notification.NotifyInput{
			EventID: ev.EventID,
			OrderID: ev.OrderID,
			Kind:    notification.KindOrderCancelled,
		})
		if err != nil {
			return err
		}
		metrics.IncEvents(consumerGroup, rec.Topic, eventType, outcomeOf(fresh))
		return nil
	}
	// Non-matching event_type: committed pass-through — absorbed (no business
	// effect), still one handled record on the metric contract.
	metrics.IncEvents(consumerGroup, rec.Topic, eventType, metrics.OutcomeAbsorbed)
	return nil
}

// outcomeOf maps the service's fresh/duplicate decision onto the pinned
// outcome label values.
func outcomeOf(fresh bool) string {
	if fresh {
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
