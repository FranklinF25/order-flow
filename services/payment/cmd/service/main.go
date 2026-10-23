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
	paymentkafka "github.com/FranklinF25/order-flow/services/payment/internal/kafka"
	"github.com/FranklinF25/order-flow/services/payment/internal/payment"
	"github.com/FranklinF25/order-flow/services/payment/internal/postgres"
)

func main() {
	addr := envOr("SERVICE_ADDR", ":8080")
	dsn := envOr("PAYMENT_DB_DSN", "postgres://payment:payment@localhost:5432/payment?sslmode=disable")
	brokers := strings.Split(envOr("KAFKA_BOOTSTRAP", "localhost:9092"), ",")
	srURL := envOr("SCHEMA_REGISTRY_URL", "http://localhost:8081")

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

	relay, err := paymentkafka.NewRelay(brokers, "payment.events", store, serde)
	if err != nil {
		log.Fatalf("relay: %v", err)
	}
	go relay.Run(ctx)

	svc := payment.NewService(store, payment.NewSimulatedGateway(), events.NewEncoder())

	consumer, err := paymentkafka.NewConsumer(brokers, svc, events.NewDecoder())
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
