package pluginsdk

import (
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

type retainingPluginStream struct {
	sent *protocol.PluginEnvelope
}

func (stream *retainingPluginStream) Send(envelope *protocol.PluginEnvelope) error {
	stream.sent = envelope
	return nil
}

func (*retainingPluginStream) Recv() (*protocol.PluginEnvelope, error) { return nil, io.EOF }
func (*retainingPluginStream) CloseSend() error                        { return nil }

func TestSenderReportsTransportFailure(t *testing.T) {
	stream := newFakePluginStream()
	want := errors.New("transport failed")
	stream.setSendError(want)
	sender := newEnvelopeSender(stream)
	_, err := sender.send(nil, &protocol.TraceContext{CorrelationId: "trace"}, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Heartbeat{Heartbeat: &protocol.Heartbeat{}}})
	if !errors.Is(err, want) {
		t.Fatalf("send error = %v; want %v", err, want)
	}
	select {
	case failure := <-sender.failure():
		if !errors.Is(failure, want) {
			t.Fatalf("reported failure = %v; want %v", failure, want)
		}
	case <-time.After(testTimeout):
		t.Fatal("sender did not publish its fatal transport failure")
	}
}

func TestSenderAllowsEnvelopeBeyondFormerProtocolLimit(t *testing.T) {
	const formerEnvelopeLimit = 64 * 1024 * 1024

	stream := &retainingPluginStream{}
	sender := newEnvelopeSender(stream)
	registered := false
	envelope := &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Log{Log: &protocol.LogRecord{
		Message: strings.Repeat("x", formerEnvelopeLimit),
	}}}
	_, err := sender.sendRegistered(nil, &protocol.TraceContext{CorrelationId: "trace"}, envelope, func(uint64) error {
		registered = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !registered {
		t.Fatal("large request was not registered as pending")
	}
	if stream.sent != envelope {
		t.Fatal("large envelope was not sent")
	}
}

func TestSenderUsesMaximumMessageIDOnce(t *testing.T) {
	stream := newFakePluginStream()
	sender := newEnvelopeSender(stream)
	sender.nextID = math.MaxUint64
	trace := &protocol.TraceContext{CorrelationId: "trace"}
	id, err := sender.send(nil, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Heartbeat{Heartbeat: &protocol.Heartbeat{}}})
	if err != nil || id != math.MaxUint64 {
		t.Fatalf("maximum ID send = %d, %v", id, err)
	}
	if _, err := sender.send(nil, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Heartbeat{Heartbeat: &protocol.Heartbeat{}}}); err == nil {
		t.Fatal("sender reused an exhausted message ID sequence")
	}
}
