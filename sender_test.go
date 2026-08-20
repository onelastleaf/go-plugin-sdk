package pluginsdk

import (
	"errors"
	"math"
	"testing"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

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

func TestSenderRejectsOversizedEnvelopeBeforeRegistration(t *testing.T) {
	stream := newFakePluginStream()
	sender := newEnvelopeSender(stream)
	registered := false
	_, err := sender.sendRegistered(nil, &protocol.TraceContext{CorrelationId: "trace"}, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ArtifactChunk{ArtifactChunk: &protocol.ArtifactTransferChunk{
		ArtifactId: &protocol.PluginArtifactId{Value: testJobID(1)},
		Data:       make([]byte, maximumEnvelopeBytes),
	}}}, func(uint64) error {
		registered = true
		return nil
	})
	if !errors.Is(err, errEnvelopeTooLarge) {
		t.Fatalf("send error = %v; want errEnvelopeTooLarge", err)
	}
	if registered {
		t.Fatal("oversized request was registered as pending")
	}
	assertNoSent(t, stream, 20*time.Millisecond)
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
