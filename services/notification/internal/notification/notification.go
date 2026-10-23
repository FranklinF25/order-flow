// Package notification holds the Notification fact: pure domain, no
// infrastructure. A notification is terminal on creation — the service
// records the fact (audit row + outbox in one tx) and hands a best-effort
// copy to RabbitMQ (topology §8; the Kafka outbox is the audit of record).
package notification

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// ErrInvalidKind is returned for notification kinds outside the avsc enum.
var ErrInvalidKind = errors.New("invalid notification kind")

// Kind is the trigger fact behind the notification
// (notification-sent.avsc enum).
type Kind string

// Notification kinds (topology v1).
const (
	KindOrderConfirmed Kind = "ORDER_CONFIRMED"
	KindOrderCancelled Kind = "ORDER_CANCELLED"
)

// Channel is the delivery medium (notification-sent.avsc enum).
type Channel string

// Delivery channels (topology v1; M2 ships EMAIL only).
const (
	ChannelEmail Channel = "EMAIL"
	ChannelSMS   Channel = "SMS"
)

// Routing keys on the orderflow.notifications topic exchange (topology §8).
const (
	RoutingKeyOrderConfirmed = "order.confirmed"
	RoutingKeyOrderCancelled = "order.cancelled"
)

// RoutingKey maps a kind to its RabbitMQ routing key (topology §8).
func (k Kind) RoutingKey() string {
	switch k {
	case KindOrderConfirmed:
		return RoutingKeyOrderConfirmed
	case KindOrderCancelled:
		return RoutingKeyOrderCancelled
	}
	return ""
}

// Notification is the audit fact of one message handed to the mailer.
type Notification struct {
	ID      string
	OrderID string
	Kind    Kind
	Channel Channel
}

// NewNotification validates and creates an EMAIL notification for orderID.
func NewNotification(id, orderID string, kind Kind) (Notification, error) {
	if id == "" {
		return Notification{}, errors.New("notification id is required")
	}
	if orderID == "" {
		return Notification{}, errors.New("order id is required")
	}
	if kind != KindOrderConfirmed && kind != KindOrderCancelled {
		return Notification{}, ErrInvalidKind
	}
	return Notification{ID: id, OrderID: orderID, Kind: kind, Channel: ChannelEmail}, nil
}

// NewUUID returns a random RFC 4122 v4 UUID string (stdlib only).
func NewUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	u := hex.EncodeToString(b[:])
	return u[0:8] + "-" + u[8:12] + "-" + u[12:16] + "-" + u[16:20] + "-" + u[20:32], nil
}
