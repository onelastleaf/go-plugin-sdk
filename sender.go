package pluginsdk

import (
	"errors"
	"math"
	"sync"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

type envelopeSender struct {
	stream     protocol.PluginRuntime_ConnectClient
	sessionID  string
	instanceID string
	nextID     uint64
	mu         sync.Mutex
}

func newEnvelopeSender(stream protocol.PluginRuntime_ConnectClient) *envelopeSender {
	return &envelopeSender{stream: stream, nextID: 1}
}

func (sender *envelopeSender) identity(sessionID, instanceID string) {
	sender.sessionID = sessionID
	sender.instanceID = instanceID
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
	sender.mu.Lock()
	defer sender.mu.Unlock()
	if sender.nextID == math.MaxUint64 {
		return 0, errors.New("plugin exhausted message IDs")
	}
	messageID := sender.nextID
	sender.nextID++
	if beforeSend != nil {
		if err := beforeSend(messageID); err != nil {
			return 0, err
		}
	}
	envelope.MessageId = messageID
	envelope.ReplyTo = replyTo
	envelope.SessionId = sender.sessionID
	envelope.PluginInstanceId = sender.instanceID
	envelope.Trace = cloneTrace(trace)
	return messageID, sender.stream.Send(envelope)
}

func ref[T any](value T) *T { return &value }
