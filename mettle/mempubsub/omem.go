// Package mempubsub provides an in-memory pubsub implementation with a concept of ordering.
// It registers for the scheme "mem".
// It supports at-least-once semantics.
//
// It is intended for local development, not production. Among other things its memory usage
// grows continually as new keys are added.
//
// # As
//
// mempubsub exposes the following types for As:
//   - Message.BeforeSend: *OrderedMessage
//   - Message: *OrderedMessage
package mempubsub

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"
)

// Scheme is the URL scheme mempubsub registers its URLOpeners under on pubsub.DefaultMux.
const Scheme = "mem"

func init() {
	uo := &urlOpener{}
	pubsub.DefaultURLMux().RegisterTopic(Scheme, uo)
	pubsub.DefaultURLMux().RegisterSubscription(Scheme, uo)
}

type urlOpener struct {
	topics sync.Map
}

// OpenTopicURL opens a pubsub.Topic based on u.
func (uo *urlOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	topicName := path.Join(u.Host, u.Path)
	t, _ := uo.topics.LoadOrStore(topicName, NewTopic())
	return t.(*pubsub.Topic), nil
}

// OpenSubscriptionURL opens a pubsub.Subscription based on u.
// For convenience, the topic will be created if it doesn't exist.
func (uo *urlOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	deadline := 10 * time.Minute
	if s := u.Query().Get("ackdeadline"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, err
		}
		deadline = d
	}
	t, _ := uo.OpenTopicURL(ctx, u)
	return NewSubscription(t, deadline), nil
}

// An OrderedMessage is like a normal Message but adds an ordering key.
type OrderedMessage struct {
	Key     string
	Message *driver.Message
}

type topic struct {
	mutex sync.Mutex
	subs  []*subscription
	ackID int64
}

// NewTopic creates a new in-memory topic.
func NewTopic() *pubsub.Topic {
	return pubsub.NewTopic(&topic{}, nil)
}

// SendBatch implements driver.Topic.SendBatch.
func (t *topic) SendBatch(ctx context.Context, messages []*driver.Message) error {
	oms := make([]*OrderedMessage, len(messages))
	for i, msg := range messages {
		msg.AckID = atomic.AddInt64(&t.ackID, 1)
		msg.AsFunc = func(interface{}) bool { return false } // Message.As panics if this is unset.
		om := &OrderedMessage{Message: msg}
		oms[i] = om
		if msg.BeforeSend != nil {
			if err := msg.BeforeSend(func(i interface{}) bool {
				if p, ok := i.(**OrderedMessage); ok {
					*p = om
					return true
				}
				return false
			}); err != nil {
				return err
			}
		}
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for _, sub := range t.subs {
		// Do this concurrently so we don't block.
		go sub.Deliver(oms)
	}
	return nil
}

// IsRetryable implements driver.Topic.IsRetryable.
func (*topic) IsRetryable(error) bool {
	return false
}

// As implements driver.Topic.As.
// We don't document the type since it's not usable externally, but NewSubscription needs it.
func (t *topic) As(i interface{}) bool {
	x, ok := i.(**topic)
	if !ok {
		return false
	}
	*x = t
	return true
}

// ErrorAs implements driver.Topic.ErrorAs
func (*topic) ErrorAs(error, interface{}) bool {
	return false
}

// ErrorCode implements driver.Topic.ErrorCode
func (*topic) ErrorCode(err error) gcerrors.ErrorCode {
	return gcerrors.Unknown
}

// Close implements driver.Topic.Close.
func (*topic) Close() error {
	return nil
}

// AddSubscription adds a new subscription to this topic.
func (t *topic) AddSubscription(sub *subscription) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.subs = append(t.subs, sub)
}

type subscription struct {
	keyedMessages sync.Map
	messages      chan *driver.Message
	acks          sync.Map
	ackDeadline   time.Duration
	closed        bool
}

// NewSubscription creates a new subscription for the given topic.
func NewSubscription(pstopic *pubsub.Topic, ackDeadline time.Duration) *pubsub.Subscription {
	var t *topic
	if !pstopic.As(&t) {
		panic("incorrect topic type")
	}
	sub := &subscription{
		messages:    make(chan *driver.Message, 1000),
		ackDeadline: ackDeadline,
	}
	t.AddSubscription(sub)
	return pubsub.NewSubscription(sub, nil, nil)
}

// ReceiveBatch implements driver.ReceiveBatch.
func (s *subscription) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	// We are lazy and only ever deliver one message at max, and just block until one appears.
	select {
	case msg := <-s.messages:
		return []*driver.Message{msg}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Deliver is used by the topic to deliver messages to this subscription
func (s *subscription) Deliver(oms []*OrderedMessage) {
	for _, om := range oms {
		ch, loaded := s.keyedMessages.LoadOrStore(om.Key, make(chan *driver.Message, 10))
		if !loaded {
			go s.forward(ch.(chan *driver.Message)) // This is a new key, we need to forward it.
		}
		s.acks.Store(om.Message.AckID, newAckableMessage(ch.(chan *driver.Message), om.Message, s.ackDeadline))
	}
}

// forward forwards messages from a subscription key to the main queue.
func (s *subscription) forward(ch chan *driver.Message) {
	defer func() {
		recover() // This happens if s.messages gets closed.
	}()
	for msg := range ch {
		s.messages <- msg
	}
}

// SendAcks implements driver.SendAcks.
func (s *subscription) SendAcks(ctx context.Context, ackIDs []driver.AckID) error {
	for _, ack := range ackIDs {
		// If it's not in the ack map that's OK (it's OK to ack a message multiple times)
		if msg, present := s.acks.Load(ack); present {
			msg.(*ackableMessage).Ack()
		}
	}
	return ctx.Err()
}

// CanNack implements driver.CanNack.
// We can't be bothered implementing nacking right now since Mettle doesn't use it.
func (s *subscription) CanNack() bool {
	return false
}

// SendNacks implements driver.SendNacks.
func (s *subscription) SendNacks(ctx context.Context, ackIDs []driver.AckID) error {
	return fmt.Errorf("SendNacks not implemented")
}

// IsRetryable implements driver.Subscription.IsRetryable.
func (*subscription) IsRetryable(error) bool {
	return false
}

// As implements driver.Subscription.As.
func (s *subscription) As(i interface{}) bool {
	return false
}

// ErrorAs implements driver.Subscription.ErrorAs
func (*subscription) ErrorAs(error, interface{}) bool {
	return false
}

// ErrorCode implements driver.Subscription.ErrorCode
func (*subscription) ErrorCode(err error) gcerrors.ErrorCode {
	return gcerrors.Unknown
}

// Close implements driver.Subscription.Close.
func (s *subscription) Close() error {
	if !s.closed {
		s.closed = true
		close(s.messages)
	}
	return nil
}

type ackableMessage struct {
	acknowledged chan struct{}
}

// newAckableMessage creates a new message and begins delivery attempts.
func newAckableMessage(delivery chan *driver.Message, msg *driver.Message, ackDeadline time.Duration) *ackableMessage {
	am := &ackableMessage{
		acknowledged: make(chan struct{}),
	}
	go am.deliver(delivery, msg, ackDeadline)
	return am
}

func (am *ackableMessage) Ack() {
	close(am.acknowledged)
}

func (am *ackableMessage) deliver(delivery chan *driver.Message, msg *driver.Message, ackDeadline time.Duration) {
	delivery <- msg
	for {
		select {
		case <-am.acknowledged:
			return
		case <-time.After(ackDeadline):
			delivery <- msg
		}
	}
}
