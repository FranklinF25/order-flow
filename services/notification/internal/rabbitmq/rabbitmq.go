// Package rabbitmq is the driven messaging adapter for the notification
// service: the orderflow.notifications topic-exchange publisher and the
// notifications.email mailer loop (topology §8). RabbitMQ is best-effort end
// to end — the Kafka NotificationSent outbox is the audit of record.
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

const (
	// ExchangeName is the durable topic exchange (topology §8).
	ExchangeName = "orderflow.notifications"
	// QueueName is the durable mailer queue (topology §8).
	QueueName = "notifications.email"
)

// DeclareTopology idempotently declares the durable topic exchange, the
// durable mailer queue and its routing-key bindings. Both the notification
// service and the mailer call it at boot: whichever process needs the
// topology first creates it, and declarations are idempotent when parameters
// match, so both tolerate the other having declared it.
func DeclareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ExchangeName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeName, err)
	}
	if _, err := ch.QueueDeclare(QueueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", QueueName, err)
	}
	for _, key := range []string{
		notification.KindOrderConfirmed.RoutingKey(),
		notification.KindOrderCancelled.RoutingKey(),
	} {
		if err := ch.QueueBind(QueueName, key, ExchangeName, false, nil); err != nil {
			return fmt.Errorf("bind %s: %w", key, err)
		}
	}
	return nil
}

type publishBody struct {
	OrderID string `json:"order_id"`
	Kind    string `json:"kind"`
}

// Publisher publishes notification intents to the topic exchange.
type Publisher struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewPublisher dials RabbitMQ and opens the publishing channel.
func NewPublisher(url string) (*Publisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	return &Publisher{conn: conn, ch: ch}, nil
}

// DeclareTopology declares exchange, queue and bindings idempotently.
func (p *Publisher) DeclareTopology() error { return DeclareTopology(p.ch) }

// Publish hands the notification to the mailer queue through the topic
// exchange: JSON body {order_id, kind}, persistent delivery, routing key per
// kind (topology §8).
func (p *Publisher) Publish(ctx context.Context, n notification.Notification) error {
	body, err := json.Marshal(publishBody{OrderID: n.OrderID, Kind: string(n.Kind)})
	if err != nil {
		return fmt.Errorf("marshal publish body: %w", err)
	}
	return p.ch.PublishWithContext(ctx, ExchangeName, n.Kind.RoutingKey(), false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

// Close releases channel and connection.
func (p *Publisher) Close() {
	_ = p.ch.Close()
	_ = p.conn.Close()
}
