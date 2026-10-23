// Package main is the notification service composition root: order.events
// consumer, RabbitMQ publisher, outbox relay and the health-only REST surface
// (topology §8).
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

	"github.com/FranklinF25/order-flow/services/notification/internal/events"
	"github.com/FranklinF25/order-flow/services/notification/internal/httpapi"
	notificationkafka "github.com/FranklinF25/order-flow/services/notification/internal/kafka"
	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
	"github.com/FranklinF25/order-flow/services/notification/internal/postgres"
	"github.com/FranklinF25/order-flow/services/notification/internal/rabbitmq"
)

func main() {
	addr := envOr("SERVICE_ADDR", ":8080")
	dsn := envOr("NOTIFICATION_DB_DSN", "postgres://notification:notification@localhost:5432/notification?sslmode=disable")
	brokers := strings.Split(envOr("KAFKA_BOOTSTRAP", "localhost:9092"), ",")
	srURL := envOr("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	rabbitURL := envOr("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := mustStore(ctx, dsn)
	defer store.Close()

	serde, err := notificationkafka.NewSerde(srURL)
	if err != nil {
		log.Fatalf("serde: %v", err)
	}
	mustEnsureSchema(ctx, serde, events.NotificationSentType, "notification-sent")

	relay, err := notificationkafka.NewRelay(brokers, "notification.events", store, serde)
	if err != nil {
		log.Fatalf("relay: %v", err)
	}
	go relay.Run(ctx)

	// DLQ producer for the retries/DLQ wrapper (topology §7): a plain
	// dedicated kgo client, closed on shutdown; the wrapper parks poison
	// records on <source-topic>.<group>.dlq and then commits the offset.
	dlq, err := notificationkafka.NewDLQProducer(brokers)
	if err != nil {
		log.Fatalf("dlq producer: %v", err)
	}
	defer dlq.Close()

	pub := mustRabbit(ctx, rabbitURL)
	defer pub.Close()

	svc := notification.NewService(store, pub, events.NewEncoder())

	consumer, err := notificationkafka.NewConsumer(brokers, svc, events.NewDecoder(), dlq)
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

	log.Printf("notification service listening on %s", addr)
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
func mustEnsureSchema(ctx context.Context, serde *notificationkafka.Serde, subject, name string) {
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

// mustRabbit dials with retries (dev stack race) and declares the exchange,
// queue and bindings idempotently — whichever process boots first creates
// them, both tolerate the other having declared it.
func mustRabbit(ctx context.Context, url string) *rabbitmq.Publisher {
	var (
		pub *rabbitmq.Publisher
		err error
	)
	for attempt := 1; attempt <= 10; attempt++ {
		pub, err = rabbitmq.NewPublisher(url)
		if err == nil {
			break
		}
		log.Printf("waiting for rabbitmq (%d/10): %v", attempt, err)
		select {
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		log.Fatalf("rabbitmq unreachable: %v", err)
	}
	if err := pub.DeclareTopology(); err != nil {
		log.Fatalf("declare topology: %v", err)
	}
	return pub
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
