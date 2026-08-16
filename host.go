package pluginsdk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pendingResponse struct {
	correlationID string
	response      chan *protocol.PluginEnvelope
}

type Host struct {
	sender                   *envelopeSender
	maximumArtifactChunkSize uint64
	maximumCallDepth         uint32
	maximumCausalDepth       uint32
	mu                       sync.Mutex
	pending                  map[uint64]pendingResponse
}

func newHost(sender *envelopeSender, maximumArtifactChunkSize uint64, maximumCallDepth, maximumCausalDepth uint32) *Host {
	return &Host{
		sender:                   sender,
		maximumArtifactChunkSize: maximumArtifactChunkSize,
		maximumCallDepth:         maximumCallDepth,
		maximumCausalDepth:       maximumCausalDepth,
		pending:                  make(map[uint64]pendingResponse),
	}
}

func (host *Host) request(ctx context.Context, trace *protocol.TraceContext, envelope *protocol.PluginEnvelope) (*protocol.PluginEnvelope, error) {
	waiter := pendingResponse{correlationID: trace.GetCorrelationId(), response: make(chan *protocol.PluginEnvelope, 1)}
	id, err := host.sender.sendRegistered(nil, trace, envelope, func(messageID uint64) error {
		host.mu.Lock()
		defer host.mu.Unlock()
		if _, exists := host.pending[messageID]; exists {
			return errors.New("duplicate pending request ID")
		}
		host.pending[messageID] = waiter
		return nil
	})
	if err != nil {
		host.removePending(id)
		return nil, err
	}
	select {
	case response := <-waiter.response:
		return response, nil
	case <-ctx.Done():
		host.removePending(id)
		return nil, ctx.Err()
	}
}

func (host *Host) removePending(id uint64) {
	host.mu.Lock()
	delete(host.pending, id)
	host.mu.Unlock()
}

func (host *Host) route(envelope *protocol.PluginEnvelope) error {
	if envelope.ReplyTo == nil {
		return errors.New("host response omitted reply_to")
	}
	host.mu.Lock()
	waiter, ok := host.pending[*envelope.ReplyTo]
	if ok {
		delete(host.pending, *envelope.ReplyTo)
	}
	host.mu.Unlock()
	if !ok {
		return errors.New("host response names no pending plugin request")
	}
	if waiter.correlationID != envelope.GetTrace().GetCorrelationId() {
		return errors.New("host response changed correlation context")
	}
	waiter.response <- envelope
	return nil
}

func (host *Host) Call(ctx context.Context, trace *protocol.TraceContext, request *protocol.HostCallRequest) (*protocol.HostCallResponse, error) {
	response, err := host.request(ctx, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_HostCall{HostCall: request}})
	if err != nil {
		return nil, err
	}
	switch value := response.Payload.(type) {
	case *protocol.PluginEnvelope_HostResult:
		if hostError := value.HostResult.GetError(); hostError != nil {
			return nil, HostError{Code: int32(hostError.Code), Message: hostError.Message, Retryable: hostError.Retryable}
		}
		return value.HostResult, nil
	case *protocol.PluginEnvelope_ProtocolError:
		return nil, HostError{Code: int32(value.ProtocolError.Code), Message: value.ProtocolError.Message, Retryable: value.ProtocolError.Retryable}
	default:
		return nil, errors.New("host call received another response kind")
	}
}

func (host *Host) InvokeConfigFunction(ctx context.Context, trace *protocol.TraceContext, function *protocol.ConfigFunctionRef, arguments []*protocol.ConfigValue) (*protocol.InvokeConfigFunctionResponse, error) {
	response, err := host.Call(ctx, trace, &protocol.HostCallRequest{Call: &protocol.HostCallRequest_InvokeConfigFunction{InvokeConfigFunction: &protocol.InvokeConfigFunctionRequest{Function: function, Arguments: arguments}}})
	if err != nil {
		return nil, err
	}
	value, ok := response.Result.(*protocol.HostCallResponse_InvokeConfigFunction)
	if !ok {
		return nil, errors.New("InvokeConfigFunction received another response kind")
	}
	return value.InvokeConfigFunction, nil
}

func (host *Host) GetConfig(ctx context.Context, trace *protocol.TraceContext, path *protocol.ConfigPath) (*protocol.GetConfigResponse, error) {
	response, err := host.Call(ctx, trace, &protocol.HostCallRequest{Call: &protocol.HostCallRequest_GetConfig{GetConfig: &protocol.GetConfigRequest{Path: path}}})
	if err != nil {
		return nil, err
	}
	value, ok := response.Result.(*protocol.HostCallResponse_GetConfig)
	if !ok {
		return nil, errors.New("GetConfig received another response kind")
	}
	return value.GetConfig, nil
}

func (host *Host) Log(trace *protocol.TraceContext, level protocol.LogLevel, target, message string, fields map[string]*protocol.ConfigValue) error {
	_, err := host.sender.send(nil, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Log{Log: &protocol.LogRecord{
		Timestamp: timestamppb.New(time.Now()), Level: level, Target: target, Message: message, Fields: fields,
	}}})
	return err
}

func (host *Host) StoreArtifact(ctx context.Context, trace *protocol.TraceContext, jobID string, descriptor *protocol.ArtifactDescriptor, chunks [][]byte) (*protocol.ArtifactStored, error) {
	if descriptor == nil || !canonicalUUIDV4(descriptor.GetArtifactId().GetValue()) ||
		descriptor.FileName == "" || descriptor.MediaType == "" || len(descriptor.Sha256) != sha256.Size {
		return nil, errors.New("artifact descriptor is invalid")
	}
	if len(chunks) == 0 {
		return nil, errors.New("artifact chunks must be nonempty")
	}
	if uint64(len(chunks)) > uint64(^uint32(0)) {
		return nil, errors.New("artifact has too many chunks")
	}
	hash := sha256.New()
	var size uint64
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			return nil, errors.New("artifact chunks must be nonempty")
		}
		if uint64(len(chunk)) > host.maximumArtifactChunkSize {
			return nil, errors.New("artifact chunk exceeds the negotiated limit")
		}
		if uint64(len(chunk)) > math.MaxUint64-size {
			return nil, errors.New("artifact size overflowed")
		}
		size += uint64(len(chunk))
		_, _ = hash.Write(chunk)
	}
	if descriptor.SizeBytes != size || !bytes.Equal(descriptor.Sha256, hash.Sum(nil)) {
		return nil, errors.New("artifact size or SHA-256 does not match its bytes")
	}

	response, err := host.request(ctx, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ArtifactStart{ArtifactStart: &protocol.ArtifactTransferStart{
		JobId: &protocol.PluginJobId{Value: jobID}, Artifact: descriptor, ChunkCount: uint32(len(chunks)),
	}}})
	if err != nil {
		return nil, err
	}
	accepted, ok := response.Payload.(*protocol.PluginEnvelope_ArtifactAccepted)
	if !ok || accepted.ArtifactAccepted.GetArtifactId().GetValue() != descriptor.GetArtifactId().GetValue() {
		return nil, errors.New("host did not accept the artifact transfer")
	}
	for index, chunk := range chunks {
		if _, err := host.sender.send(nil, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ArtifactChunk{ArtifactChunk: &protocol.ArtifactTransferChunk{
			ArtifactId: descriptor.ArtifactId, ChunkIndex: uint32(index), Data: chunk,
		}}}); err != nil {
			return nil, fmt.Errorf("send artifact chunk %d: %w", index, err)
		}
	}
	response, err = host.request(ctx, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ArtifactComplete{ArtifactComplete: &protocol.ArtifactTransferComplete{ArtifactId: descriptor.ArtifactId}}})
	if err != nil {
		return nil, err
	}
	stored, ok := response.Payload.(*protocol.PluginEnvelope_ArtifactStored)
	if !ok || stored.ArtifactStored.GetArtifactId().GetValue() != descriptor.GetArtifactId().GetValue() {
		return nil, errors.New("host did not acknowledge the stored artifact")
	}
	return stored.ArtifactStored, nil
}
