package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/order/internal/events"
	"github.com/FranklinF25/order-flow/services/order/internal/order"
)

// Handler processes one consumed record; its offset commits only after it
// returns nil (topology §7 at-least-once).
type Handler func(ctx context.Context, rec *kgo.Record) error

// Chain combines the handlers subscribed to one stream (topology §6: one
// consumer group serves several event types). Each handler filters on the
// event_type header, so non-matching records pass through untouched; the
// first error stops the record uncommitted.
func Chain(handlers ...Handler) Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		for _, h := range handlers {
			if err := h(ctx, rec); err != nil {
				return err
			}
		}
		return nil
	}
}

// Consumer polls one stream in one group. M1: no retries/DLQ — on a handler
// error it stops uncommitted so the record is redelivered on the next start.
type Consumer struct {
	client  *kgo.Client
	handler Handler
}

// NewConsumer builds a stream consumer (auto.offset.reset=earliest, explicit
// commits after each handled record).
func NewConsumer(brokers []string, topic, group string, handler Handler) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Consumer{client: cl, handler: handler}, nil
}

// Run polls until ctx is cancelled or a handler fails (M1 stop-on-error).
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
			if err := c.client.CommitRecords(ctx, rec); err != nil && ctx.Err() == nil {
				log.Printf("commit %s[%d] offset %d failed: %v (M1: no retry/DLQ)", rec.Topic, rec.Partition, rec.Offset, err)
				c.client.Close()
				return
			}
		}
	}
	c.client.Close()
}

// StockReservedHandler reacts only to inventory.events StockReserved facts
// (topology §6 subscriptions; other event types never reach the domain).
func StockReservedHandler(svc *order.Service) Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		if headerValue(rec, "event_type") != events.StockReservedType {
			return nil
		}
		payload, err := StripWire(rec.Value)
		if err != nil {
			return err
		}
		ev, err := events.DecodeStockReserved(payload)
		if err != nil {
			return fmt.Errorf("decode StockReserved: %w", err)
		}
		return svc.OnStockReserved(ctx, ev.EventID, ev.OrderID)
	}
}

// StockUnavailableHandler reacts only to inventory.events StockUnavailable
// facts (compensation A): AWAITING_STOCK → CANCELLED(STOCK_UNAVAILABLE).
func StockUnavailableHandler(svc *order.Service) Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		if headerValue(rec, "event_type") != events.StockUnavailableType {
			return nil
		}
		payload, err := StripWire(rec.Value)
		if err != nil {
			return err
		}
		ev, err := events.DecodeStockUnavailable(payload)
		if err != nil {
			return fmt.Errorf("decode StockUnavailable: %w", err)
		}
		return svc.OnStockUnavailable(ctx, ev.EventID, ev.OrderID)
	}
}

// PaymentCapturedHandler reacts only to payment.events PaymentCaptured facts.
func PaymentCapturedHandler(svc *order.Service) Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		if headerValue(rec, "event_type") != events.PaymentCapturedType {
			return nil
		}
		payload, err := StripWire(rec.Value)
		if err != nil {
			return err
		}
		ev, err := events.DecodePaymentCaptured(payload)
		if err != nil {
			return fmt.Errorf("decode PaymentCaptured: %w", err)
		}
		return svc.OnPaymentCaptured(ctx, ev.EventID, ev.OrderID)
	}
}

// PaymentFailedHandler reacts only to payment.events PaymentFailed facts
// (compensation B): AWAITING_PAYMENT → CANCELLED(PAYMENT_FAILED).
func PaymentFailedHandler(svc *order.Service) Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		if headerValue(rec, "event_type") != events.PaymentFailedType {
			return nil
		}
		payload, err := StripWire(rec.Value)
		if err != nil {
			return err
		}
		ev, err := events.DecodePaymentFailed(payload)
		if err != nil {
			return fmt.Errorf("decode PaymentFailed: %w", err)
		}
		return svc.OnPaymentFailed(ctx, ev.EventID, ev.OrderID)
	}
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
