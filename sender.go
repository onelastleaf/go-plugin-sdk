package pluginsdk

import (
	"errors"
	"fmt"
	"math"
	"sync"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

type pluginStream interface {
	Send(*protocol.PluginEnvelope) error
	Recv() (*protocol.PluginEnvelope, error)
	CloseSend() error
}

type envelopeSender struct {
	stream     pluginStream
	sessionID  string
	instanceID string
	nextID     uint64
	exhausted  bool

	mu          sync.Mutex
	failureOnce sync.Once
	failures    chan error
}

func newEnvelopeSender(stream pluginStream) *envelopeSender {
	return &envelopeSender{stream: stream, nextID: 1, failures: make(chan error, 1)}
}

func (sender *envelopeSender) identity(sessionID, instanceID string) {
	sender.mu.Lock()
	sender.sessionID = sessionID
	sender.instanceID = instanceID
	sender.mu.Unlock()
}

func (sender *envelopeSender) failure() <-chan error { return sender.failures }

func (sender *envelopeSender) reportFailure(err error) {
	if err == nil {
		return
	}
	sender.failureOnce.Do(func() { sender.failures <- err })
}

func (sender *envelopeSender) send(replyTo *uint64, trace *protocol.TraceContext, envelope *protocol.PluginEnvelope) (uint64, error) {
	return sender.sendRegistered(replyTo, trace, envelope, nil)
}

func (sender *envelopeSender) sendRegistered(
	replyTo *uint64,
	trace *protocol.TraceContext,
	envelope *protocol.PluginEnvelope,
	beforeSend func(uint64) error,
) (uint64, error) {
	if envelope == nil {
		return 0, errors.New("plugin envelope must not be nil")
	}
	sender.mu.Lock()
	defer sender.mu.Unlock()
	if sender.exhausted {
		return 0, errors.New("plugin exhausted message IDs")
	}
	messageID := sender.nextID
	if messageID == math.MaxUint64 {
		sender.exhausted = true
	} else {
		sender.nextID++
	}

	envelope.MessageId = messageID
	if replyTo == nil {
		envelope.ReplyTo = nil
	} else {
		envelope.ReplyTo = ref(*replyTo)
	}
	envelope.SessionId = sender.sessionID
	envelope.PluginInstanceId = sender.instanceID
	envelope.Trace = cloneTrace(trace)
	if beforeSend != nil {
		// Registration and Send share this lock, so a fast host response cannot
		// arrive before its waiter is visible to the receive loop.
		if err := beforeSend(messageID); err != nil {
			return messageID, err
		}
	}
	if err := sender.stream.Send(envelope); err != nil {
		err = fmt.Errorf("send plugin envelope: %w", err)
		sender.reportFailure(err)
		return messageID, err
	}
	return messageID, nil
}

func ref[T any](value T) *T { return &value }
