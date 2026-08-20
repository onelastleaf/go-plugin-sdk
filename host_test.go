package pluginsdk

import (
	"context"
	"errors"
	"testing"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
)

func TestLateResponseToCancelledHostCallIsConsumed(t *testing.T) {
	stream := newFakePluginStream()
	sender := newEnvelopeSender(stream)
	host := newHostClient(sender, 1024, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	trace := &protocol.TraceContext{CorrelationId: "trace", ParentCallId: ref(uint64(4)), CallDepth: 2}
	go func() {
		_, err := host.request(ctx, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_HostCall{HostCall: &protocol.HostCallRequest{Call: &protocol.HostCallRequest_GetConfig{GetConfig: &protocol.GetConfigRequest{}}}}})
		done <- err
	}()
	request := nextSent(t, stream)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("request error = %v; want context cancellation", err)
	}
	response := &protocol.PluginEnvelope{
		ReplyTo: ref(request.MessageId),
		Trace:   cloneTrace(trace),
		Payload: &protocol.PluginEnvelope_HostResult{HostResult: &protocol.HostCallResponse{Result: &protocol.HostCallResponse_GetConfig{GetConfig: &protocol.GetConfigResponse{}}}},
	}
	if err := host.route(response); err != nil {
		t.Fatalf("late response broke the session: %v", err)
	}
	if count := host.activePendingCount(); count != 0 {
		t.Fatalf("pending count = %d; want 0", count)
	}
}

func TestHostCloseFailsPendingCalls(t *testing.T) {
	stream := newFakePluginStream()
	host := newHostClient(newEnvelopeSender(stream), 1024, 8, 8)
	done := make(chan error, 1)
	go func() {
		_, err := host.request(context.Background(), &protocol.TraceContext{CorrelationId: "trace"}, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_HostCall{HostCall: &protocol.HostCallRequest{Call: &protocol.HostCallRequest_GetConfig{GetConfig: &protocol.GetConfigRequest{}}}}})
		done <- err
	}()
	_ = nextSent(t, stream)
	want := errors.New("session failed")
	host.close(want)
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("pending request error = %v; want %v", err, want)
		}
	case <-time.After(testTimeout):
		t.Fatal("pending request was not released when the host client closed")
	}
	if host.activePendingCount() != 0 {
		t.Fatal("host close retained pending requests")
	}
}

func TestHostResponseMustPreserveCompleteTrace(t *testing.T) {
	stream := newFakePluginStream()
	host := newHostClient(newEnvelopeSender(stream), 1024, 8, 8)
	done := make(chan error, 1)
	trace := &protocol.TraceContext{CorrelationId: "trace", ParentCallId: ref(uint64(1)), CallDepth: 1, CausalDepth: 2}
	go func() {
		_, err := host.request(context.Background(), trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_HostCall{HostCall: &protocol.HostCallRequest{Call: &protocol.HostCallRequest_GetConfig{GetConfig: &protocol.GetConfigRequest{}}}}})
		done <- err
	}()
	request := nextSent(t, stream)
	changed := cloneTrace(trace)
	changed.CausalDepth++
	routeErr := host.route(&protocol.PluginEnvelope{ReplyTo: ref(request.MessageId), Trace: changed, Payload: &protocol.PluginEnvelope_HostResult{HostResult: &protocol.HostCallResponse{}}})
	if routeErr == nil {
		t.Fatal("accepted a host response with a changed trace")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("waiting host call accepted the mismatched response")
		}
	case <-time.After(testTimeout):
		t.Fatal("mismatched response did not release its waiter")
	}
}

func TestActionContextAlwaysBuildsNestedTrace(t *testing.T) {
	stream := newFakePluginStream()
	host := newHostClient(newEnvelopeSender(stream), 1024, 8, 8)
	action := newActionContext(context.Background(), testJobID(1), &protocol.TraceContext{CorrelationId: "trace", CallDepth: 2}, 42, host)
	done := make(chan error, 1)
	go func() {
		_, err := action.GetConfig(nil)
		done <- err
	}()
	request := nextSent(t, stream)
	if request.Trace.CallDepth != 3 || request.Trace.GetParentCallId() != 42 {
		t.Fatalf("nested trace = %v; want depth 3 and parent 42", request.Trace)
	}
	if err := host.route(&protocol.PluginEnvelope{
		ReplyTo: ref(request.MessageId),
		Trace:   cloneTrace(request.Trace),
		Payload: &protocol.PluginEnvelope_HostResult{HostResult: &protocol.HostCallResponse{Result: &protocol.HostCallResponse_GetConfig{GetConfig: &protocol.GetConfigResponse{}}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestActionLogPreservesJobTrace(t *testing.T) {
	stream := newFakePluginStream()
	host := newHostClient(newEnvelopeSender(stream), 1024, 8, 8)
	trace := &protocol.TraceContext{CorrelationId: "trace", ParentCallId: ref(uint64(7)), CallDepth: 2, CausalDepth: 3}
	action := newActionContext(context.Background(), testJobID(1), trace, 42, host)
	if err := action.Log(protocol.LogLevel_LOG_LEVEL_INFO, "test", "message", nil); err != nil {
		t.Fatal(err)
	}
	envelope := nextSent(t, stream)
	if envelope.GetLog() == nil || !proto.Equal(envelope.Trace, trace) {
		t.Fatalf("log trace = %v; want unchanged job trace %v", envelope.Trace, trace)
	}
}
