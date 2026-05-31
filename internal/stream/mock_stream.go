package stream

import (
	"context"
	"sync"
)

// MockTargetPublish records a single PublishToSubject call.
type MockTargetPublish struct {
	Subject  string
	DedupKey string
	Event    *Publish
}

// MockStream is a test-double for Stream that records publishes and allows
// injecting messages for consumption.
type MockStream struct {
	mu                 sync.RWMutex
	published          []*Publish
	publishedToSubject []MockTargetPublish
	err                error

	sub *MockSubscription
}

// NewMockStream creates a new mock stream.
func NewMockStream() *MockStream {
	return &MockStream{
		sub: NewMockSubscription(),
	}
}

// Publish records the event and returns the configured error.
func (m *MockStream) Publish(_ context.Context, event *Publish) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, event)
	return m.err
}

// PublishToSubject records the subject, dedup key, and event, and returns the
// configured error.
func (m *MockStream) PublishToSubject(_ context.Context, subject string, dedupKey string, event *Publish) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedToSubject = append(m.publishedToSubject, MockTargetPublish{
		Subject:  subject,
		DedupKey: dedupKey,
		Event:    event,
	})
	return m.err
}

// PublishedToSubjects returns all entries recorded by PublishToSubject in
// call order.
func (m *MockStream) PublishedToSubjects() []MockTargetPublish {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]MockTargetPublish, len(m.publishedToSubject))
	copy(out, m.publishedToSubject)
	return out
}

// Subscribe returns a mock subscription.
func (m *MockStream) Subscribe(_ string) (Subscription, error) {
	return m.sub, nil
}

// SetError configures the error returned by Publish.
func (m *MockStream) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Published returns all events passed to Publish.
func (m *MockStream) Published() []*Publish {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Publish, len(m.published))
	copy(out, m.published)
	return out
}

// Subscription returns the underlying mock subscription for test setup.
func (m *MockStream) Subscription() *MockSubscription {
	return m.sub
}

// MockSubscription is a test-double for Subscription.
type MockSubscription struct {
	mu       sync.Mutex
	messages []*Message
	closed   bool
	err      error
}

// NewMockSubscription creates a new mock subscription.
func NewMockSubscription() *MockSubscription {
	return &MockSubscription{}
}

// Enqueue adds messages that will be returned by Fetch.
func (m *MockSubscription) Enqueue(msgs ...*Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msgs...)
}

// Fetch returns up to batch messages from the queue.
func (m *MockSubscription) Fetch(_ context.Context, batch int) ([]*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return nil, m.err
	}
	if m.closed {
		return nil, ErrSubscriptionClosed
	}

	n := batch
	if n > len(m.messages) {
		n = len(m.messages)
	}
	msgs := m.messages[:n]
	m.messages = m.messages[n:]
	return msgs, nil
}

// Close marks the subscription as closed.
func (m *MockSubscription) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// SetError configures the error returned by Fetch.
func (m *MockSubscription) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Remaining returns the count of messages not yet fetched.
func (m *MockSubscription) Remaining() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}
