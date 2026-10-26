// Package main is the payment service composition root: simulated gateway,
// inventory.events consumer, outbox relay and the health-only REST surface.
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

	"github.com/FranklinF25/order-flow/services/payment/internal/events"
	"github.com/FranklinF25/order-flow/services/payment/internal/httpapi"
	"github.com/FranklinF25/order-flow/services/payment/internal/metrics"
	paymentkafka "github.com/FranklinF25/order-flow/services/payment/internal/kafka"
	"github.com/FranklinF25/order-flow/services/payment/internal/payment"
	"github.com/FranklinF25/order-flow/services/payment/internal/postgres"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := envOr("SERVICE_ADDR", ":8080")
	dsn := envOr("PAYMENT_DB_DSN", "postgres://payment:payment@localhost:5432/payment?sslmode=disable")
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

	serde, err := paymentkafka.NewSerde(srURL)
	if err != nil {
		log.Fatalf("serde: %v", err)
	}
	mustEnsureSchema(ctx, serde, events.PaymentCapturedType, "payment-captured")
	mustEnsureSchema(ctx, serde, events.PaymentFailedType, "payment-failed")

	// One metrics instance serves the whole process: the relay, the consumer
	// and GET /metrics (default registry) all share it.
	m := metrics.New()

	relay, err := paymentkafka.NewRelay(brokers, "payment.events", store, serde, m)
	if err != nil {
		log.Fatalf("relay: %v", err)
	}
	go relay.Run(ctx)

	// DLQ producer for the retries/DLQ wrapper (topology §7): a plain
	// dedicated kgo client, closed on shutdown; the wrapper parks poison
	// records on <source-topic>.<group>.dlq and then commits the offset.
	dlq, err := paymentkafka.NewDLQProducer(brokers)
	if err != nil {
		log.Fatalf("dlq producer: %v", err)
	}
	defer dlq.Close()

	svc := payment.NewService(store, payment.NewSimulatedGateway(), events.NewEncoder())

	consumer, err := paymentkafka.NewConsumer(brokers, svc, events.NewDecoder(),
		paymentkafka.NewRedisGate(rdb, "payment.inventory-events"), dlq, m)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	go consumer.Run(ctx)

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("payment service listening on %s", addr)
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

// mustEnsureSchema registers the produced record schema (SR settles late).
func mustEnsureSchema(ctx context.Context, serde *paymentkafka.Serde, subject, name string) {
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
