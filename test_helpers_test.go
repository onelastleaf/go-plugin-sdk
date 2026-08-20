package pluginsdk

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
)

const testTimeout = 3 * time.Second

type receiveResult struct {
	envelope *protocol.PluginEnvelope
	err      error
}

type fakePluginStream struct {
	mu       sync.Mutex
	sendErr  error
	sent     chan *protocol.PluginEnvelope
	receive  chan receiveResult
	closed   chan struct{}
	closeOne sync.Once
}

func newFakePluginStream() *fakePluginStream {
	return &fakePluginStream{
		sent:    make(chan *protocol.PluginEnvelope, 2048),
		receive: make(chan receiveResult, 16),
		closed:  make(chan struct{}),
	}
}

func (stream *fakePluginStream) Send(envelope *protocol.PluginEnvelope) error {
	stream.mu.Lock()
	err := stream.sendErr
	stream.mu.Unlock()
	if err != nil {
		return err
	}
	stream.sent <- proto.Clone(envelope).(*protocol.PluginEnvelope)
	return nil
}

func (stream *fakePluginStream) Recv() (*protocol.PluginEnvelope, error) {
	value, ok := <-stream.receive
	if !ok {
		return nil, io.EOF
	}
	return value.envelope, value.err
}

func (stream *fakePluginStream) CloseSend() error {
	stream.closeOne.Do(func() { close(stream.closed) })
	return nil
}

func (stream *fakePluginStream) setSendError(err error) {
	stream.mu.Lock()
	stream.sendErr = err
	stream.mu.Unlock()
}

func nextSent(t *testing.T, stream *fakePluginStream) *protocol.PluginEnvelope {
	t.Helper()
	select {
	case envelope := <-stream.sent:
		return envelope
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for a plugin envelope")
		return nil
	}
}

func assertNoSent(t *testing.T, stream *fakePluginStream, duration time.Duration) {
	t.Helper()
	select {
	case envelope := <-stream.sent:
		t.Fatalf("unexpected plugin envelope: %T", envelope.Payload)
	case <-time.After(duration):
	}
}

type serveHarness struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stream   *fakePluginStream
	sender   *envelopeSender
	host     *hostClient
	incoming chan receivedEnvelope
	done     chan struct{}

	mu         sync.Mutex
	err        error
	nextHostID uint64
}

func newServeHarness(plugin *Plugin) *serveHarness {
	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakePluginStream()
	sender := newEnvelopeSender(stream)
	sender.identity("session", "instance")
	host := newHostClient(sender, artifactReadBufferBytes, 16, 16)
	harness := &serveHarness{
		ctx:        ctx,
		cancel:     cancel,
		stream:     stream,
		sender:     sender,
		host:       host,
		incoming:   make(chan receivedEnvelope, 2048),
		done:       make(chan struct{}),
		nextHostID: 1,
	}
	lastHostID := uint64(0)
	go func() {
		err := plugin.serve(ctx, stream, harness.incoming, sender, host, &lastHostID)
		cancel()
		host.close(err)
		harness.mu.Lock()
		harness.err = err
		harness.mu.Unlock()
		close(harness.done)
	}()
	return harness
}

func (harness *serveHarness) send(envelope *protocol.PluginEnvelope, trace *protocol.TraceContext, replyTo *uint64) uint64 {
	if trace == nil {
		trace = &protocol.TraceContext{CorrelationId: fmt.Sprintf("trace-%d", harness.nextHostID)}
	}
	id := harness.nextHostID
	harness.nextHostID++
	envelope.MessageId = id
	envelope.ReplyTo = replyTo
	envelope.SessionId = "session"
	envelope.PluginInstanceId = "instance"
	envelope.Trace = cloneTrace(trace)
	harness.incoming <- receivedEnvelope{envelope: envelope}
	return id
}

func (harness *serveHarness) stop() error {
	harness.cancel()
	select {
	case <-harness.done:
	case <-time.After(testTimeout):
		return errorsForTest("serve did not stop")
	}
	harness.mu.Lock()
	defer harness.mu.Unlock()
	return harness.err
}

func (harness *serveHarness) wait() error {
	select {
	case <-harness.done:
	case <-time.After(testTimeout):
		return errorsForTest("serve did not finish")
	}
	harness.mu.Lock()
	defer harness.mu.Unlock()
	return harness.err
}

type errorsForTest string

func (value errorsForTest) Error() string { return string(value) }

func testJobID(index int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", index)
}

func startJobEnvelope(id, action string) *protocol.PluginEnvelope {
	return &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_StartJob{StartJob: &protocol.StartJobRequest{
		JobId: &protocol.PluginJobId{Value: id},
		Invocation: &protocol.StartJobRequest_Action{Action: &protocol.ActionInvocation{
			Action: action,
		}},
	}}}
}
