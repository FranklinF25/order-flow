// Package main is the simulated mailer (topology §8): it consumes the
// notifications.email queue with manual acks, "sends" with one structured log
// line per message, and requeues failures up to 3 attempts before
// log-and-drop. No RabbitMQ DLX in M2 — notifications are best-effort and
// audited via NotificationSent on the Kafka side.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/FranklinF25/order-flow/services/notification/internal/rabbitmq"
)

func main() {
	url := envOr("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, ch := mustRabbit(ctx, url)
	defer conn.Close()
	defer ch.Close()

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("qos: %v", err)
	}

	mailer := rabbitmq.NewMailer(
		rabbitmq.NewLoggedSender(),
		func(p amqp.Publishing) error {
			// Bounded retry without a DLX: requeue at the tail of the mailer
			// queue with an incremented attempt header.
			return ch.PublishWithContext(ctx, "", rabbitmq.QueueName, false, false, p)
		},
	)

	deliveries, err := ch.Consume(rabbitmq.QueueName, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume %s: %v", rabbitmq.QueueName, err)
	}

	log.Printf("mailer consuming %s", rabbitmq.QueueName)
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			mailer.Handle(d)
		}
	}
}

// mustRabbit dials with retries (dev stack race) and declares the exchange,
// queue and bindings idempotently — whichever process boots first creates
// them, both tolerate the other having declared it.
func mustRabbit(ctx context.Context, url string) (*amqp.Connection, *amqp.Channel) {
	var (
		conn *amqp.Connection
		err  error
	)
	for attempt := 1; attempt <= 10; attempt++ {
		conn, err = amqp.Dial(url)
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
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("rabbitmq channel: %v", err)
	}
	if err := rabbitmq.DeclareTopology(ch); err != nil {
		log.Fatalf("declare topology: %v", err)
	}
	return conn, ch
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
