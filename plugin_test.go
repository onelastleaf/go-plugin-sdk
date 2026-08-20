package pluginsdk

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type delayedEOFParent struct {
	release   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func (parent *delayedEOFParent) Read([]byte) (int, error) {
	<-parent.release
	return 0, io.EOF
}

func (parent *delayedEOFParent) Close() error {
	parent.closeOnce.Do(func() { close(parent.closed) })
	return nil
}

func TestBuilderProducesStableImmutablePlugin(t *testing.T) {
	handler := func(ActionContext, []string) (ActionResult, error) { return ActionResult{}, nil }
	builder := New("dev.example.plugin", "0.1.0").
		Action("zeta", "last", handler).
		Action("alpha", "first", handler)
	plugin, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	builder.Action("later", "must not leak into built plugin", handler)

	if got := []string{plugin.actionDescriptors[0].Name, plugin.actionDescriptors[1].Name}; got[0] != "alpha" || got[1] != "zeta" {
		t.Fatalf("action descriptors are not stable: %v", got)
	}
	if _, exists := plugin.actions["later"]; exists {
		t.Fatal("built plugin changed when its builder was reused")
	}
}

func TestBuilderValidationAndStringResult(t *testing.T) {
	if _, err := New("invalid", "0.1.0").Build(); err == nil {
		t.Fatal("accepted invalid plugin ID")
	}
	if _, err := New("dev.example.plugin", "0.1.0").Action("bad", "", nil).Build(); err == nil {
		t.Fatal("accepted a nil action handler")
	}
	if got := StringResult("value").String(); got != "value" {
		t.Fatalf("got %q", got)
	}
	if !canonicalUUIDV4("0f337c0c-51d6-44a9-a691-a31fce775ab1") || canonicalUUIDV4("0f337c0c-51d6-14a9-a691-a31fce775ab1") {
		t.Fatal("canonical UUID v4 validation is incorrect")
	}
}

func TestEndpointMustBeExplicitLoopbackWithoutCredentials(t *testing.T) {
	invalid := []string{
		"https://127.0.0.1:1",
		"http://example.com:1",
		"http://127.0.0.1",
		"http://user:password@127.0.0.1:1",
		"http://127.0.0.1:0",
		"http://127.0.0.1:65536",
		"http://127.0.0.1:1/",
		"http://127.0.0.1:1?query",
	}
	for _, value := range invalid {
		if _, err := validateEndpoint(value); err == nil {
			t.Errorf("accepted %q", value)
		}
	}
	valid := map[string]string{
		"http://127.0.0.1:1": "127.0.0.1:1",
		"http://localhost:2": "localhost:2",
		"http://[::1]:3":     "[::1]:3",
	}
	for value, expected := range valid {
		actual, err := validateEndpoint(value)
		if err != nil || actual != expected {
			t.Errorf("validateEndpoint(%q) = %q, %v; want %q", value, actual, err, expected)
		}
	}
}

func TestSessionReadyMustPreserveCompleteTrace(t *testing.T) {
	expected := &protocol.TraceContext{CorrelationId: "same", ParentCallId: ref(uint64(7)), CallDepth: 1, CausalDepth: 2, TaskId: ref("task")}
	ready := &protocol.PluginEnvelope{Trace: cloneTrace(expected), Payload: &protocol.PluginEnvelope_Ready{Ready: &protocol.SessionReady{}}}
	if err := validateSessionReady(ready, expected); err != nil {
		t.Fatal(err)
	}
	ready.Trace.ParentCallId = ref(uint64(8))
	if err := validateSessionReady(ready, expected); err == nil {
		t.Fatal("accepted SessionReady with a changed parent_call_id")
	}
}

func TestStructuredErrorsPreserveMetadataAndDetails(t *testing.T) {
	detail := &anypb.Any{TypeUrl: "type.example/detail", Value: []byte("payload")}
	actionErr := ActionError{
		Code:      protocol.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
		Message:   "bad input",
		Retryable: true,
		Metadata:  map[string]string{"field": "value"},
		Details:   []*anypb.Any{detail},
	}
	converted := actionProtocolError(actionErr)
	if converted.Code != actionErr.Code || !converted.Retryable || converted.Metadata["field"] != "value" || !proto.Equal(converted.Details[0], detail) {
		t.Fatalf("structured action error was flattened: %v", converted)
	}

	hostErr := newHostError(converted)
	converted.Metadata["field"] = "mutated"
	copyOne := hostErr.Detail()
	copyOne.Metadata["field"] = "also-mutated"
	if hostErr.Code() != actionErr.Code || !hostErr.Retryable() || hostErr.Detail().Metadata["field"] != "value" {
		t.Fatal("HostError did not retain an independent complete detail")
	}
	var matched *HostError
	if !errors.As(hostErr, &matched) {
		t.Fatal("HostError cannot be matched with errors.As")
	}
}

func TestEnvelopeDepthLimits(t *testing.T) {
	lastID := uint64(0)
	envelope := &protocol.PluginEnvelope{MessageId: 1, Trace: &protocol.TraceContext{CorrelationId: "test", CallDepth: 2}}
	if err := validateEnvelope(envelope, &lastID, "", "", 1, 1); err == nil {
		t.Fatal("envelope above the negotiated call depth was accepted")
	}
}

func TestRunAtDoesNotWaitForParentReaderAfterContextCancellation(t *testing.T) {
	plugin, err := New("dev.example.plugin", "0.1.0").Build()
	if err != nil {
		t.Fatal(err)
	}
	parent := &delayedEOFParent{release: make(chan struct{}), closed: make(chan struct{})}
	defer close(parent.release)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- plugin.runAt(ctx, "http://127.0.0.1:1", parent)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runAt error = %v; want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runAt waited for the parent reader after closing it")
	}
	select {
	case <-parent.closed:
	default:
		t.Fatal("runAt did not close the parent-liveness stream")
	}
}
