package kafka

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/order/internal/metrics"
)

// maxHandlerAttempts is the §7 blocking-retry budget: 1 initial attempt + 2
// retries, all in-process (no retry topics — deferred by topology §9).
const maxHandlerAttempts = 3

// RetryBaseDelay is the production exponential-backoff base: the three
// attempts run at t=0, t=100ms and t=300ms. Tests inject 0 instead so the
// blocking path stays instant.
const RetryBaseDelay = 100 * time.Millisecond

// DLQProducer is the driven port that parks poison records on a consumer's
// dead-letter topic (topology §7). Produce is synchronous: the wrapper only
// commits the source offset after the DLQ ack.
type DLQProducer interface {
	Produce(ctx context.Context, rec *kgo.Record) error
	Close()
}

// KgoDLQProducer implements DLQProducer on a plain franz-go client:
// acks=all, idempotent produce (franz-go's default), raw bytes — no
// schema-registry/avro involvement. It is a small dedicated client, closed on
// service shutdown, independent from the outbox relay's client lifetime.
type KgoDLQProducer struct {
	client *kgo.Client
}

// NewDLQProducer builds the dead-letter producer.
func NewDLQProducer(brokers []string) (*KgoDLQProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("dlq producer: %w", err)
	}
	return &KgoDLQProducer{client: cl}, nil
}

// Produce synchronously writes one record.
func (p *KgoDLQProducer) Produce(ctx context.Context, rec *kgo.Record) error {
	for _, res := range p.client.ProduceSync(ctx, rec) {
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

// Close releases the producer (service shutdown).
func (p *KgoDLQProducer) Close() { p.client.Close() }

// dlqTopic derives `<source-topic>.<consumer-group>.dlq` (topology §7; the
// topics themselves are created out of band).
func dlqTopic(sourceTopic, group string) string {
	return sourceTopic + "." + group + ".dlq"
}

// dlqRecord clones the record onto its DLQ topic: raw value bytes and key as-is,
// original headers preserved, plus the dlq.* audit headers.
func dlqRecord(rec *kgo.Record, topic string, lastErr error) *kgo.Record {
	out := &kgo.Record{
		Topic: topic,
		Key:   rec.Key,   // key preserved
		Value: rec.Value, // raw bytes unchanged — no registry, no re-encode
		Headers: append(append([]kgo.RecordHeader{}, rec.Headers...),
			kgo.RecordHeader{Key: "dlq.original-topic", Value: []byte(rec.Topic)},
			kgo.RecordHeader{Key: "dlq.partition", Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
			kgo.RecordHeader{Key: "dlq.offset", Value: []byte(strconv.FormatInt(rec.Offset, 10))},
			kgo.RecordHeader{Key: "dlq.error", Value: []byte(lastErr.Error())},
			kgo.RecordHeader{Key: "dlq.attempts", Value: []byte(strconv.Itoa(maxHandlerAttempts))},
		),
	}
	if et := headerValue(rec, "event_type"); et != "" {
		out.Headers = append(out.Headers, kgo.RecordHeader{Key: "dlq.event-type", Value: []byte(et)})
	}
	return out
}

// WithRetries wraps a Handler with §7's blocking retries + DLQ: on error the
// record is retried in-process up to 3 total attempts with exponential
// backoff (baseDelay, then 2×baseDelay); if the 3rd attempt still fails, the
// RAW record is published to `<source-topic>.<group>.dlq` with the dlq.*
// audit headers and the wrapper returns nil so the consumer COMMITS the
// offset — the record is parked on the DLQ and the stream continues. A DLQ
// produce failure returns the error instead: the offset stays uncommitted and
// redelivery re-runs the whole path (at-least-once into the DLQ).
//
// Metrics (M3 contract): every blocked attempt — a failure that enters the
// backoff — counts orderflow_retries_total{service,group}; the parking
// attempt instead counts orderflow_dlq_parked_total{service,group,topic} and
// marks the record absorbed for the outer WithMetrics (a parked record never
// advanced the saga; the DLQ counter is the alert-bearing series).
//
// Composition matters: the fast-gate must sit OUTSIDE this wrapper
// (WithFastGate(gate, WithRetries(...))) so an absorbed replay never burns
// the retry budget, and exactly one retry mechanism wraps each consumer.
func WithRetries(dlq DLQProducer, m *metrics.Metrics, baseDelay time.Duration, group string, next Handler) Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		var err error
		for attempt := 1; attempt <= maxHandlerAttempts; attempt++ {
			if err = next(ctx, rec); err == nil {
				return nil
			}
			log.Printf("%s: attempt %d/%d failed: %v", group, attempt, maxHandlerAttempts, err)
			if attempt < maxHandlerAttempts {
				if m != nil {
					m.IncRetries(group) // blocked attempt: it now waits out the backoff
				}
				delay := baseDelay << (attempt - 1)
				select {
				case <-ctx.Done():
					return ctx.Err() // shutdown mid-backoff: uncommitted, redelivered
				case <-time.After(delay):
				}
			}
		}
		topic := dlqTopic(rec.Topic, group)
		if perr := dlq.Produce(ctx, dlqRecord(rec, topic, err)); perr != nil {
			log.Printf("%s: DLQ produce %s failed: %v (offset stays uncommitted)", group, topic, perr)
			return fmt.Errorf("dlq produce after %d attempts: %w", maxHandlerAttempts, perr)
		}
		log.Printf("%s: parked %s[%d] offset %d (event %s) on %s after %d failed attempts",
			group, rec.Topic, rec.Partition, rec.Offset, headerValue(rec, "event_id"), topic, maxHandlerAttempts)
		if m != nil {
			m.IncDLQParked(group, rec.Topic)
			metrics.MarkAbsorbed(ctx) // parked ≠ saga progress; the outer wrapper counts it absorbed
		}
		return nil // offset commits: the poison record is parked, the stream continues
	}
}
