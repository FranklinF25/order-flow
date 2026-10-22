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
	"github.com/FranklinF25/order-flow/services/order/internal/order"
	"github.com/FranklinF25/order-flow/services/order/internal/postgres"
)

func main() {
	addr := envOr("SERVICE_ADDR", ":8080")
	dsn := envOr("ORDER_DB_DSN", "postgres://order:order@localhost:5432/order?sslmode=disable")
	brokers := strings.Split(envOr("KAFKA_BOOTSTRAP", "localhost:9092"), ",")
	srURL := envOr("SCHEMA_REGISTRY_URL", "http://localhost:8081")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := mustStore(ctx, dsn)
	defer store.Close()

	serde, err := orderkafka.NewSerde(srURL)
	if err != nil {
		log.Fatalf("serde: %v", err)
	}
	mustEnsureSchema(ctx, serde, events.OrderPlacedType, "order-placed")
	mustEnsureSchema(ctx, serde, events.OrderConfirmedType, "order-confirmed")

	relay, err := orderkafka.NewRelay(brokers, "order.events", store, serde)
	if err != nil {
		log.Fatalf("relay: %v", err)
	}
	go relay.Run(ctx)

	svc := order.NewService(store, store, events.NewEncoder())
	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(svc),
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
