package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

// maxDeliveryAttempts bounds the requeues before log-and-drop: a failing
// delivery is requeued up to 3 attempts, then dropped — notifications are
// best-effort and audited via NotificationSent on the Kafka side (topology
// §8, no RabbitMQ DLX in M2).
const maxDeliveryAttempts = 3

// attemptHeader carries the requeue count across redeliveries.
const attemptHeader = "x-attempt"

// Mail is the decoded wire shape of a mailer message.
type Mail struct {
	OrderID string
	Kind    notification.Kind
}

// Sender is the delivery port; the simulated sender logs one structured line.
type Sender interface {
	Send(m Mail) error
}

// LoggedSender is the M2 simulated sender: it "delivers" by logging.
type LoggedSender struct{}

// NewLoggedSender builds the simulated sender.
func NewLoggedSender() LoggedSender { return LoggedSender{} }

// Send simulates the email delivery with one structured log line.
func (LoggedSender) Send(m Mail) error {
	log.Printf("mailer: email sent kind=%s order_id=%s", m.Kind, m.OrderID)
	return nil
}

// Mailer processes notifications.email deliveries: decode → simulated send →
// manual ack.
type Mailer struct {
	sender Sender
	// republish requeues a message on the mailer queue with an incremented
	// attempt header (the bounded-retry stand-in for a DLX).
	republish func(amqp.Publishing) error
}

// NewMailer wires the mailer; republish is the requeue channel operation.
func NewMailer(sender Sender, republish func(amqp.Publishing) error) *Mailer {
	return &Mailer{sender: sender, republish: republish}
}

// Handle processes one delivery: decode → simulated send → manual ack. A
// decode or transient send failure requeues the message at the tail of the
// queue with an incremented x-attempt header (bounded, no DLX); after
// maxDeliveryAttempts the message is dropped with a log — best-effort per
// topology §8, audited via NotificationSent on the Kafka side.
func (m *Mailer) Handle(d amqp.Delivery) {
	mail, err := decodeMail(d.Body)
	if err == nil {
		err = m.sender.Send(mail)
	}
	if err == nil {
		_ = d.Ack(false)
		return
	}
	attempts := deliveryAttempts(d.Headers)
	if attempts >= maxDeliveryAttempts {
		log.Printf("mailer: dropping message after %d attempts: %v (best-effort, audited via NotificationSent)", attempts, err)
		_ = d.Ack(false)
		return
	}
	log.Printf("mailer: attempt %d failed, requeueing: %v", attempts+1, err)
	if rerr := m.republish(amqp.Publishing{
		Headers:      amqp.Table{attemptHeader: int32(attempts + 1)},
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         d.Body,
	}); rerr != nil {
		log.Printf("mailer: requeue publish failed, dropping: %v (best-effort)", rerr)
	}
	_ = d.Ack(false)
}

func decodeMail(body []byte) (Mail, error) {
	var raw struct {
		OrderID string `json:"order_id"`
		Kind    string `json:"kind"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return Mail{}, fmt.Errorf("decode mail body: %w", err)
	}
	if raw.OrderID == "" || raw.Kind == "" {
		return Mail{}, errors.New("mail body missing order_id or kind")
	}
	return Mail{OrderID: raw.OrderID, Kind: notification.Kind(raw.Kind)}, nil
}

func deliveryAttempts(h amqp.Table) int {
	switch n := h[attemptHeader].(type) {
	case int32:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}
