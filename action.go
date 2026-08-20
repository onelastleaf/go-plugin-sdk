package pluginsdk

import (
	"context"
	"errors"
	"io"
	"sync"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
)

// ArtifactInput describes an artifact whose bytes will be read from Source.
// Source must be seekable because the SDK hashes it before streaming it to oll.
type ArtifactInput struct {
	ID        string
	FileName  string
	MediaType string
	Source    io.ReadSeeker
}

// ActionContext is scoped to one action invocation. It must not be retained or
// used after the handler returns.
type ActionContext interface {
	// Context is cancelled when oll cancels the job, its deadline expires, or
	// the plugin session ends.
	Context() context.Context
	// JobID returns the canonical job UUID assigned by oll.
	JobID() string
	// Trace returns an independent copy of the action's trace context.
	Trace() *protocol.TraceContext
	// HostCall invokes an oll capability permitted for this plugin.
	HostCall(*protocol.HostCallRequest) (*protocol.HostCallResponse, error)
	// GetConfig reads from the plugin's live, oll-owned configuration.
	GetConfig(*protocol.ConfigPath) (*protocol.GetConfigResponse, error)
	// InvokeConfigFunction invokes a session-scoped configuration function.
	InvokeConfigFunction(*protocol.ConfigFunctionRef, []*protocol.ConfigValue) (*protocol.InvokeConfigFunctionResponse, error)
	// Log writes a structured record using the unchanged job trace.
	Log(protocol.LogLevel, string, string, map[string]*protocol.ConfigValue) error
	// StoreArtifact hashes and streams an artifact using the unchanged job
	// trace, returning its descriptor only after oll acknowledges storage.
	StoreArtifact(ArtifactInput) (*protocol.ArtifactDescriptor, error)
}

type actionContext struct {
	ctx          context.Context
	jobID        string
	trace        *protocol.TraceContext
	parentCallID uint64
	host         *hostClient

	artifactMu sync.Mutex
	artifacts  map[string]*protocol.ArtifactDescriptor
}

func newActionContext(ctx context.Context, jobID string, trace *protocol.TraceContext, parentCallID uint64, host *hostClient) *actionContext {
	return &actionContext{
		ctx:          ctx,
		jobID:        jobID,
		trace:        cloneTrace(trace),
		parentCallID: parentCallID,
		host:         host,
		artifacts:    make(map[string]*protocol.ArtifactDescriptor),
	}
}

func (value *actionContext) Context() context.Context      { return value.ctx }
func (value *actionContext) JobID() string                 { return value.jobID }
func (value *actionContext) Trace() *protocol.TraceContext { return cloneTrace(value.trace) }

func (value *actionContext) nestedTrace() (*protocol.TraceContext, error) {
	trace := cloneTrace(value.trace)
	trace.ParentCallId = ref(value.parentCallID)
	if trace.CallDepth == ^uint32(0) {
		return nil, errors.New("host-call depth overflowed")
	}
	trace.CallDepth++
	if trace.CallDepth > value.host.maximumCallDepth {
		return nil, errors.New("host call exceeds the negotiated call-depth limit")
	}
	return trace, nil
}

func (value *actionContext) HostCall(request *protocol.HostCallRequest) (*protocol.HostCallResponse, error) {
	trace, err := value.nestedTrace()
	if err != nil {
		return nil, err
	}
	return value.host.call(value.ctx, trace, request)
}

func (value *actionContext) GetConfig(path *protocol.ConfigPath) (*protocol.GetConfigResponse, error) {
	trace, err := value.nestedTrace()
	if err != nil {
		return nil, err
	}
	return value.host.getConfig(value.ctx, trace, path)
}

func (value *actionContext) InvokeConfigFunction(function *protocol.ConfigFunctionRef, arguments []*protocol.ConfigValue) (*protocol.InvokeConfigFunctionResponse, error) {
	trace, err := value.nestedTrace()
	if err != nil {
		return nil, err
	}
	return value.host.invokeConfigFunction(value.ctx, trace, function, arguments)
}

func (value *actionContext) Log(level protocol.LogLevel, target, message string, fields map[string]*protocol.ConfigValue) error {
	return value.host.log(value.ctx, cloneTrace(value.trace), level, target, message, fields)
}

func (value *actionContext) StoreArtifact(input ArtifactInput) (_ *protocol.ArtifactDescriptor, returnErr error) {
	if err := validateArtifactInput(input); err != nil {
		return nil, err
	}
	value.artifactMu.Lock()
	if _, exists := value.artifacts[input.ID]; exists {
		value.artifactMu.Unlock()
		return nil, errors.New("artifact ID is already in use by this action")
	}
	value.artifacts[input.ID] = nil
	value.artifactMu.Unlock()
	defer func() {
		if returnErr != nil {
			value.artifactMu.Lock()
			delete(value.artifacts, input.ID)
			value.artifactMu.Unlock()
		}
	}()

	descriptor, err := value.host.storeArtifact(value.ctx, cloneTrace(value.trace), value.jobID, input)
	if err != nil {
		return nil, err
	}
	value.artifactMu.Lock()
	value.artifacts[input.ID] = cloneArtifactDescriptor(descriptor)
	value.artifactMu.Unlock()
	return cloneArtifactDescriptor(descriptor), nil
}

func (value *actionContext) cloneAndValidateResult(result ActionResult) (ActionResult, error) {
	cloned := ActionResult{}
	if result.Result != nil {
		cloned.Result = proto.Clone(result.Result).(*protocol.ConfigValue)
	}
	cloned.Artifacts = make([]*protocol.ArtifactDescriptor, 0, len(result.Artifacts))
	seen := make(map[string]struct{}, len(result.Artifacts))
	value.artifactMu.Lock()
	defer value.artifactMu.Unlock()
	for _, descriptor := range result.Artifacts {
		if descriptor == nil {
			return ActionResult{}, errors.New("action result contains a nil artifact descriptor")
		}
		id := descriptor.GetArtifactId().GetValue()
		stored, ok := value.artifacts[id]
		if !ok || stored == nil || !proto.Equal(stored, descriptor) {
			return ActionResult{}, errors.New("action result contains an artifact not stored by this action")
		}
		if _, duplicate := seen[id]; duplicate {
			return ActionResult{}, errors.New("action result contains a duplicate artifact")
		}
		seen[id] = struct{}{}
		cloned.Artifacts = append(cloned.Artifacts, cloneArtifactDescriptor(descriptor))
	}
	return cloned, nil
}

func cloneTrace(value *protocol.TraceContext) *protocol.TraceContext {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*protocol.TraceContext)
}

func cloneArtifactDescriptor(value *protocol.ArtifactDescriptor) *protocol.ArtifactDescriptor {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*protocol.ArtifactDescriptor)
}
