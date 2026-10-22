// Package kafka is the driven messaging adapter for the order service:
// Schema Registry serde with the Confluent wire format and the outbox relay.
package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

// Serde caches schema ids per subject (RecordNameStrategy: subject == full
// record name) and encodes the Confluent wire format:
// magic byte 0x00 + int32 big-endian schema id + Avro payload.
type Serde struct {
	client *sr.Client

	mu  sync.Mutex
	ids map[string]int
}

// NewSerde builds the Schema Registry client.
func NewSerde(url string) (*Serde, error) {
	client, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		return nil, fmt.Errorf("schema registry client: %w", err)
	}
	return &Serde{client: client, ids: map[string]int{}}, nil
}

// Ensure registers (idempotently for identical schemas) and caches the id.
func (s *Serde) Ensure(ctx context.Context, subject, schema string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ids[subject]; ok {
		return nil
	}
	info, err := s.client.CreateSchema(ctx, subject, sr.Schema{Schema: schema, Type: sr.TypeAvro})
	if err != nil {
		return fmt.Errorf("ensure schema %s: %w", subject, err)
	}
	s.ids[subject] = info.ID
	return nil
}

// SchemaID returns the cached id for a subject.
func (s *Serde) SchemaID(subject string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.ids[subject]
	if !ok {
		return 0, fmt.Errorf("schema id for %s not ensured", subject)
	}
	return id, nil
}

// Encode wraps the payload in the Confluent wire format.
func (s *Serde) Encode(schemaID int, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = 0x00
	binary.BigEndian.PutUint32(out[1:5], uint32(schemaID))
	copy(out[5:], payload)
	return out
}

// StripWire removes the Confluent wire envelope (magic 0x00 + int32 BE schema
// id) and returns the Avro body. Decoding uses the EMBEDDED schema; the
// registry id is advisory in M1 (documented choice, topology §4).
func StripWire(value []byte) ([]byte, error) {
	if len(value) < 5 {
		return nil, fmt.Errorf("wire payload too short: %d bytes", len(value))
	}
	if value[0] != 0x00 {
		return nil, fmt.Errorf("unsupported wire format magic byte %#x", value[0])
	}
	return value[5:], nil
}
