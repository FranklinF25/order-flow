// Package main is the order service composition root.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/FranklinF25/order-flow/services/order/internal/events"
	"github.com/FranklinF25/order-flow/services/order/internal/httpapi"
	orderkafka "github.com/FranklinF25/order-flow/services/order/internal/kafka"
	"github.com/FranklinF25/order-flow/services/order/internal/metrics"
	"github.com/FranklinF25/order-flow/services/order/internal/order"
	"github.com/FranklinF25/order-flow/services/order/internal/postgres"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := envOr("SERVICE_ADDR", ":8080")
	dsn := envOr("ORDER_DB_DSN", "postgres://order:order@localhost:5432/order?sslmode=disable")
	brokers := strings.Split(envOr("KAFKA_BOOTSTRAP", "localhost:9092"), ",")
	srURL := envOr("SCHEMA_REGISTRY_URL", "http://localhost:8081")

	// Redis idempotency fast-gate (topology §7, first gate): empty/unset
	// REDIS_URL disables it (the DB processed_events guard stays the real
	// gate); a malformed URL fails fast. Short timeouts keep the gate
	// best-effort — an unavailable Redis degrades to the normal handler
	// path instead of stalling consumption.
	var rdb *redis.Client
	if envOr("REDIS_URL", "") == "" {
		log.Printf("redis fast-gate disabled")
	} else {
		opts, err := redis.ParseURL(os.Getenv("REDIS_URL"))
		if err != nil {
			log.Fatalf("redis fast-gate: %v", err)
		}
		opts.DialTimeout = 2 * time.Second
		opts.ReadTimeout = time.Second
		opts.WriteTimeout = time.Second
		opts.MaxRetries = 1
		rdb = redis.NewClient(opts)
		log.Printf("redis fast-gate enabled")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := mustStore(ctx, dsn)
	defer store.Close()

	// Pinned orderflow metrics (M3): served on the existing httpapi mux at
	// GET /metrics; every component below instruments through this instance.
	m := metrics.New()
	statusNames := make([]string, len(order.Statuses))
	for i, s := range order.Statuses {
		statusNames[i] = string(s)
	}
	go m.RunSagaOrdersPoller(ctx, statusNames, 15*time.Second, store.CountOrdersByStatus)

	serde, err := orderkafka.NewSerde(srURL)
	if err != nil {
		log.Fatalf("serde: %v", err)
	}
	mustEnsureSchema(ctx, serde, events.OrderPlacedType, "order-placed")
	mustEnsureSchema(ctx, serde, events.OrderConfirmedType, "order-confirmed")
	mustEnsureSchema(ctx, serde, events.OrderCancelledType, "order-cancelled")

	relay, err := orderkafka.NewRelay(brokers, "order.events", store, serde, m)
	if err != nil {
		log.Fatalf("relay: %v", err)
	}
	go relay.Run(ctx)

	// DLQ producer for the retries/DLQ wrapper (topology §7): a plain
	// dedicated kgo client, closed on shutdown; the wrapper parks poison
	// records on <source-topic>.<group>.dlq and then commits the offset.
	dlq, err := orderkafka.NewDLQProducer(brokers)
	if err != nil {
		log.Fatalf("dlq producer: %v", err)
	}
	defer dlq.Close()

	svc := order.NewService(store, store, events.NewEncoder())

	// Saga subscriptions (topology §6): order reacts to inventory.events
	// (StockReserved + StockUnavailable) and payment.events (PaymentCaptured +
	// PaymentFailed), each stream in its own consumer group. Composition
	// (outside-in): fast-gate → retries/DLQ → chained handlers — a gate
	// absorb returns before any retry attempt is burned.
	invConsumer, err := orderkafka.NewConsumer(brokers, "inventory.events", "order.inventory-events",
		orderkafka.WithMetrics(m, "order.inventory-events",
			orderkafka.WithFastGate(
				orderkafka.NewRedisGate(rdb, "order.inventory-events"),
				orderkafka.WithRetries(dlq, m, orderkafka.RetryBaseDelay, "order.inventory-events",
					orderkafka.Chain(orderkafka.StockReservedHandler(svc), orderkafka.StockUnavailableHandler(svc))))))
	if err != nil {
		log.Fatalf("inventory consumer: %v", err)
	}
	go invConsumer.Run(ctx)

	payConsumer, err := orderkafka.NewConsumer(brokers, "payment.events", "order.payment-events",
		orderkafka.WithMetrics(m, "order.payment-events",
			orderkafka.WithFastGate(
				orderkafka.NewRedisGate(rdb, "order.payment-events"),
				orderkafka.WithRetries(dlq, m, orderkafka.RetryBaseDelay, "order.payment-events",
					orderkafka.Chain(orderkafka.PaymentCapturedHandler(svc), orderkafka.PaymentFailedHandler(svc))))))
	if err != nil {
		log.Fatalf("payment consumer: %v", err)
	}
	go payConsumer.Run(ctx)
	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(svc, m.Handler()),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("order service listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// mustStore connects with retries (dev stack race) and migrates at boot.
func mustStore(ctx context.Context, dsn string) *postgres.Store {
	var (
		store *postgres.Store
		err   error
	)
	for attempt := 1; attempt <= 10; attempt++ {
		store, err = postgres.Connect(ctx, dsn)
		if err == nil {
			break
		}
		log.Printf("waiting for database (%d/10): %v", attempt, err)
		select {
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		log.Fatalf("database unreachable: %v", err)
	}
	if err := store.Migrate(dsn); err != nil {
		log.Fatalf("migrations: %v", err)
	}
	return store
}

// mustEnsureSchema registers the produced record schemas (SR settles late).
func mustEnsureSchema(ctx context.Context, serde *orderkafka.Serde, subject, name string) {
	text, err := events.SchemaText(name)
	if err != nil {
		log.Fatalf("schema %s: %v", name, err)
	}
	for attempt := 1; attempt <= 10; attempt++ {
		err = serde.Ensure(ctx, subject, text)
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	log.Fatalf("schema registry unreachable: %v", err)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
