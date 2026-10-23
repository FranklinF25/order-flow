package rabbitmq

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

// fakeAcker records manual acknowledgements for a delivery.
type fakeAcker struct {
	acks  int
	nacks int
}

func (f *fakeAcker) Ack(_ uint64, _ bool) error     { f.acks++; return nil }
func (f *fakeAcker) Nack(_ uint64, _, _ bool) error { f.nacks++; return nil }
func (f *fakeAcker) Reject(_ uint64, _ bool) error  { f.nacks++; return nil }

// fakeSender records simulated sends and can simulate a transient failure.
type fakeSender struct {
	mail []Mail
	fail bool
}

func (f *fakeSender) Send(m Mail) error {
	if f.fail {
		return errors.New("smtp down")
	}
	f.mail = append(f.mail, m)
	return nil
}

// Triangulation: a decode failure requeues with an incremented x-attempt
// header; after maxDeliveryAttempts the message is dropped (acked, no
// requeue) — no RabbitMQ DLX in M2 (topology §8).
func TestMailerRequeuesBoundedThenDrops(t *testing.T) {
	acker := &fakeAcker{}
	sender := &fakeSender{}
	var republished []amqp.Publishing
	mailer := NewMailer(sender, func(p amqp.Publishing) error {
		republished = append(republished, p)
		return nil
	})

	// First failure: requeued once with x-attempt=1, original acked.
	mailer.Handle(amqp.Delivery{
		Headers:      amqp.Table{},
		Body:         []byte(`{not json`),
		Acknowledger: acker,
	})
	if acker.acks != 1 || len(republished) != 1 {
		t.Fatalf("after first failure: acks=%d republished=%d, want 1/1", acker.acks, len(republished))
	}
	if got := republished[0].Headers[attemptHeader]; got != int32(1) {
		t.Errorf("requeue x-attempt = %v, want 1", got)
	}
	if len(sender.mail) != 0 {
		t.Errorf("undecodable mail must not reach the sender, got %+v", sender.mail)
	}

	// Failure at maxDeliveryAttempts: acked and dropped, no further requeue.
	mailer.Handle(amqp.Delivery{
		Headers:      amqp.Table{attemptHeader: int32(maxDeliveryAttempts)},
		Body:         []byte(`{not json`),
		Acknowledger: acker,
	})
	if acker.acks != 2 || len(republished) != 1 {
		t.Fatalf("after drop: acks=%d republished=%d, want 2/1", acker.acks, len(republished))
	}
}

// Triangulation: a transient sender failure follows the same bounded path
// and requeues the original payload as a persistent message.
func TestMailerTransientFailureRequeues(t *testing.T) {
	acker := &fakeAcker{}
	sender := &fakeSender{fail: true}
	var republished []amqp.Publishing
	mailer := NewMailer(sender, func(p amqp.Publishing) error {
		republished = append(republished, p)
		return nil
	})

	mailer.Handle(amqp.Delivery{
		Headers:      amqp.Table{},
		Body:         []byte(`{"order_id":"ord-1","kind":"ORDER_CANCELLED"}`),
		Acknowledger: acker,
	})
	if acker.acks != 1 || len(republished) != 1 {
		t.Fatalf("acks=%d republished=%d, want 1/1", acker.acks, len(republished))
	}
	if got := string(republished[0].Body); got != `{"order_id":"ord-1","kind":"ORDER_CANCELLED"}` {
		t.Errorf("requeued body = %q, want the original payload", got)
	}
	if republished[0].DeliveryMode != amqp.Persistent {
		t.Errorf("requeued delivery mode = %d, want persistent", republished[0].DeliveryMode)
	}
}

// Rule 3: the mailer decodes {order_id, kind}, simulates the send with one
// structured log line (via the sender) and acks the delivery exactly once.
func TestMailerDecodesSendsAndAcks(t *testing.T) {
	acker := &fakeAcker{}
	sender := &fakeSender{}
	var republished []amqp.Publishing
	mailer := NewMailer(sender, func(p amqp.Publishing) error {
		republished = append(republished, p)
		return nil
	})

	mailer.Handle(amqp.Delivery{
		Headers:      amqp.Table{},
		Body:         []byte(`{"order_id":"ord-1","kind":"ORDER_CONFIRMED"}`),
		Acknowledger: acker,
	})

	if len(sender.mail) != 1 {
		t.Fatalf("simulated sends = %d, want 1", len(sender.mail))
	}
	m := sender.mail[0]
	if m.OrderID != "ord-1" || m.Kind != notification.KindOrderConfirmed {
		t.Errorf("mail = %+v, want ord-1/ORDER_CONFIRMED", m)
	}
	if acker.acks != 1 {
		t.Errorf("acks = %d, want 1", acker.acks)
	}
	if acker.nacks != 0 {
		t.Errorf("nacks = %d, want 0", acker.nacks)
	}
	if len(republished) != 0 {
		t.Errorf("republished = %d, want 0 for a healthy delivery", len(republished))
	}
}
