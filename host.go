package pluginsdk

import (
	"context"
	"errors"
	"sync"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type hostResponse struct {
	envelope *protocol.PluginEnvelope
	err      error
}

type pendingResponse struct {
	trace     *protocol.TraceContext
	response  chan hostResponse
	abandoned bool
}

type hostClient struct {
	sender                   *envelopeSender
	maximumArtifactChunkSize uint64
	maximumCallDepth         uint32
	maximumCausalDepth       uint32

	mu       sync.Mutex
	pending  map[uint64]*pendingResponse
	closed   bool
	closeErr error
	changes  chan struct{}
}

func newHostClient(sender *envelopeSender, maximumArtifactChunkSize uint64, maximumCallDepth, maximumCausalDepth uint32) *hostClient {
	return &hostClient{
		sender:                   sender,
		maximumArtifactChunkSize: maximumArtifactChunkSize,
		maximumCallDepth:         maximumCallDepth,
		maximumCausalDepth:       maximumCausalDepth,
		pending:                  make(map[uint64]*pendingResponse),
		changes:                  make(chan struct{}, 1),
	}
}

func (host *hostClient) request(ctx context.Context, trace *protocol.TraceContext, envelope *protocol.PluginEnvelope) (*protocol.PluginEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	waiter := &pendingResponse{
		trace:    cloneTrace(trace),
		response: make(chan hostResponse, 1),
	}
	id, err := host.sender.sendRegistered(nil, trace, envelope, func(messageID uint64) error {
		host.mu.Lock()
		defer host.mu.Unlock()
		if host.closed {
			return host.closeErr
		}
		if _, exists := host.pending[messageID]; exists {
			return errors.New("duplicate pending request ID")
		}
		host.pending[messageID] = waiter
		return nil
	})
	if err != nil {
		host.removePending(id, waiter)
		return nil, err
	}

	select {
	case response := <-waiter.response:
		return response.envelope, response.err
	case <-ctx.Done():
		changed := false
		host.mu.Lock()
		if host.pending[id] == waiter {
			// Keep the correlation slot until the late response arrives. Removing
			// it here would turn a legitimate response into a protocol failure.
			waiter.abandoned = true
			changed = true
		}
		host.mu.Unlock()
		if changed {
			host.notifyChange()
		}
		return nil, ctx.Err()
	}
}

func (host *hostClient) removePending(id uint64, expected *pendingResponse) {
	if id == 0 {
		return
	}
	removed := false
	host.mu.Lock()
	if host.pending[id] == expected {
		delete(host.pending, id)
		removed = true
	}
	host.mu.Unlock()
	if removed {
		host.notifyChange()
	}
}

func (host *hostClient) route(envelope *protocol.PluginEnvelope) error {
	if envelope.ReplyTo == nil {
		return ProtocolError{"host response omitted reply_to"}
	}
	host.mu.Lock()
	waiter, ok := host.pending[*envelope.ReplyTo]
	if ok {
		delete(host.pending, *envelope.ReplyTo)
	}
	host.mu.Unlock()
	if !ok {
		return ProtocolError{"host response names no pending plugin request"}
	}
	host.notifyChange()
	if !proto.Equal(waiter.trace, envelope.Trace) {
		err := ProtocolError{"host response changed trace context"}
		if !waiter.abandoned {
			waiter.response <- hostResponse{err: err}
		}
		return err
	}
	if !waiter.abandoned {
		waiter.response <- hostResponse{envelope: envelope}
	}
	return nil
}

func (host *hostClient) close(err error) {
	if err == nil {
		err = errors.New("plugin session closed")
	}
	host.mu.Lock()
	if host.closed {
		host.mu.Unlock()
		return
	}
	host.closed = true
	host.closeErr = err
	pending := host.pending
	host.pending = make(map[uint64]*pendingResponse)
	host.mu.Unlock()
	host.notifyChange()
	for _, waiter := range pending {
		if !waiter.abandoned {
			waiter.response <- hostResponse{err: err}
		}
	}
}

func (host *hostClient) activePendingCount() int {
	host.mu.Lock()
	defer host.mu.Unlock()
	count := 0
	for _, waiter := range host.pending {
		if !waiter.abandoned {
			count++
		}
	}
	return count
}

func (host *hostClient) changeEvents() <-chan struct{} { return host.changes }

func (host *hostClient) notifyChange() {
	select {
	case host.changes <- struct{}{}:
	default:
	}
}

func (host *hostClient) call(ctx context.Context, trace *protocol.TraceContext, request *protocol.HostCallRequest) (*protocol.HostCallResponse, error) {
	if request == nil || request.Call == nil {
		return nil, errors.New("host call request is required")
	}
	response, err := host.request(ctx, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_HostCall{HostCall: request}})
	if err != nil {
		return nil, err
	}
	switch value := response.Payload.(type) {
	case *protocol.PluginEnvelope_HostResult:
		if value.HostResult == nil {
			return nil, errors.New("host call returned an empty result")
		}
		if hostError := value.HostResult.GetError(); hostError != nil {
			return nil, newHostError(hostError)
		}
		return value.HostResult, nil
	case *protocol.PluginEnvelope_ProtocolError:
		return nil, newHostError(value.ProtocolError)
	default:
		return nil, ProtocolError{"host call received another response kind"}
	}
}

func (host *hostClient) invokeConfigFunction(ctx context.Context, trace *protocol.TraceContext, function *protocol.ConfigFunctionRef, arguments []*protocol.ConfigValue) (*protocol.InvokeConfigFunctionResponse, error) {
	response, err := host.call(ctx, trace, &protocol.HostCallRequest{Call: &protocol.HostCallRequest_InvokeConfigFunction{InvokeConfigFunction: &protocol.InvokeConfigFunctionRequest{Function: function, Arguments: arguments}}})
	if err != nil {
		return nil, err
	}
	value, ok := response.Result.(*protocol.HostCallResponse_InvokeConfigFunction)
	if !ok {
		return nil, ProtocolError{"InvokeConfigFunction received another response kind"}
	}
	return value.InvokeConfigFunction, nil
}

func (host *hostClient) getConfig(ctx context.Context, trace *protocol.TraceContext, path *protocol.ConfigPath) (*protocol.GetConfigResponse, error) {
	response, err := host.call(ctx, trace, &protocol.HostCallRequest{Call: &protocol.HostCallRequest_GetConfig{GetConfig: &protocol.GetConfigRequest{Path: path}}})
	if err != nil {
		return nil, err
	}
	value, ok := response.Result.(*protocol.HostCallResponse_GetConfig)
	if !ok {
		return nil, ProtocolError{"GetConfig received another response kind"}
	}
	return value.GetConfig, nil
}

func (host *hostClient) log(ctx context.Context, trace *protocol.TraceContext, level protocol.LogLevel, target, message string, fields map[string]*protocol.ConfigValue) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clonedFields := make(map[string]*protocol.ConfigValue, len(fields))
	for key, value := range fields {
		if value != nil {
			clonedFields[key] = proto.Clone(value).(*protocol.ConfigValue)
		}
	}
	_, err := host.sender.send(nil, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Log{Log: &protocol.LogRecord{
		Timestamp: timestamppb.New(time.Now()),
		Level:     level,
		Target:    target,
		Message:   message,
		Fields:    clonedFields,
	}}})
	return err
}
