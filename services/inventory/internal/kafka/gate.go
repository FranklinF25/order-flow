package kafka

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler processes one consumed record; its offset commits only after it
// returns nil (topology §7 at-least-once). The fast-gate wraps it before
// dispatch.
type Handler func(ctx context.Context, rec *kgo.Record) error

// gateTTL is the fast-gate replay-absorption window (topology §7: TTL 72h).
const gateTTL = 259200 * time.Second

// FastGate is the driven idempotency port of the Redis fast-gate (topology
// §7, first gate): Seen reports whether this consumer already processed an
// event, Mark records it. It is a replay-ABSORBING CACHE in front of the real
// guard — the processed_events insert inside each handler transaction — never
// a claim: Mark may only be called after the handler's transaction committed.
type FastGate interface {
	Seen(ctx context.Context, eventID string) (bool, error)
	Mark(ctx context.Context, eventID string) error
}

// RedisGate implements FastGate on go-redis. Keys are scoped per consumer
// group (processed:<group>:<event_id>): several groups consume the same event
// ids (order and payment both consume StockReserved; order and inventory both
// consume PaymentFailed), and one group's mark must never absorb another
// group's processing — each gate mirrors its own processed_events guard.
type RedisGate struct {
	client *redis.Client
	scope  string // consumer group
}

// NewRedisGate builds the adapter for one consumer group. A nil client (the
// REDIS_URL-unset case) disables the gate: NewRedisGate returns nil and
// WithFastGate passes records straight through.
func NewRedisGate(client *redis.Client, scope string) FastGate {
	if client == nil {
		return nil
	}
	return &RedisGate{client: client, scope: scope}
}

func (g *RedisGate) key(eventID string) string {
	return "processed:" + g.scope + ":" + eventID
}

// Seen checks processed:<group>:<event_id> (EXISTS).
func (g *RedisGate) Seen(ctx context.Context, eventID string) (bool, error) {
	n, err := g.client.Exists(ctx, g.key(eventID)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Mark sets processed:<group>:<event_id> = "1" with the 72h TTL.
func (g *RedisGate) Mark(ctx context.Context, eventID string) error {
	return g.client.Set(ctx, g.key(eventID), "1", gateTTL).Err()
}

// WithFastGate wraps a Handler with the Redis fast-gate, before dispatch
// (topology §7 first gate). Crash-safe ordering:
//
//	hit   → absorb: skip the handler and return nil, so the offset commits
//	        exactly like the DB-guard duplicate path
//	miss  → run the handler; ONLY after it returns nil (tx committed) Mark —
//	        a premature mark plus a crash before the commit would permanently
//	        lose the event
//	error → log one warn line and proceed to the handler (availability beats
//	        fastness; the DB guard makes it safe). Mark failures after a
//	        committed tx also only log.
//
// A nil gate (REDIS_URL unset) returns the handler unchanged. Records without
// an event_id header bypass the gate untouched (the producer stamps it from
// the same EventID the payload carries; the DB guard owns their safety).
func WithFastGate(gate FastGate, next Handler) Handler {
	if gate == nil {
		return next
	}
	return func(ctx context.Context, rec *kgo.Record) error {
		eventID := headerValue(rec, "event_id")
		if eventID == "" {
			return next(ctx, rec)
		}
		seen, err := gate.Seen(ctx, eventID)
		if err != nil {
			log.Printf("fast-gate: check %s: %v (proceeding to handler)", eventID, err)
		} else if seen {
			return nil // absorbed: already processed; the offset commits below
		}
		if err := next(ctx, rec); err != nil {
			return err // tx not committed: never mark; redelivery retries
		}
		if err := gate.Mark(ctx, eventID); err != nil {
			log.Printf("fast-gate: mark %s: %v (tx committed, ignoring)", eventID, err)
		}
		return nil
	}
}
