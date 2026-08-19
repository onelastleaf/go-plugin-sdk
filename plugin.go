package pluginsdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
)

const ProtocolSchemaSHA256 = "9b236b37455965858413f5717a88e28568a459e81e87a28ff77be8845bcff75a"

type ActionResult struct {
	Result    *protocol.ConfigValue
	Artifacts []*protocol.ArtifactDescriptor
}

func StringResult(value string) ActionResult {
	return ActionResult{Result: &protocol.ConfigValue{Kind: &protocol.ConfigValue_StringValue{StringValue: value}}}
}

func (result ActionResult) String() string {
	if result.Result == nil {
		return ""
	}
	return result.Result.GetStringValue()
}

type ActionHandler func(ActionContext, []string) (ActionResult, error)

type action struct {
	description string
	handler     ActionHandler
}

type Plugin struct {
	id      string
	version string
	actions map[string]action
	err     error
}

func New(id, version string) *Plugin {
	return &Plugin{id: id, version: version, actions: make(map[string]action)}
}

func (plugin *Plugin) Action(name, description string, handler ActionHandler) *Plugin {
	if plugin.err != nil {
		return plugin
	}
	if name == "" || handler == nil {
		plugin.err = errors.New("action name and handler are required")
		return plugin
	}
	if _, exists := plugin.actions[name]; exists {
		plugin.err = fmt.Errorf("duplicate action %q", name)
		return plugin
	}
	plugin.actions[name] = action{description: description, handler: handler}
	return plugin
}

func (plugin *Plugin) Build() (*Plugin, error) {
	if plugin.err != nil {
		return nil, plugin.err
	}
	if err := validatePluginID(plugin.id); err != nil {
		return nil, err
	}
	if plugin.version == "" {
		return nil, errors.New("plugin version must not be empty")
	}
	return plugin, nil
}

func (plugin *Plugin) Run(ctx context.Context) error {
	if _, err := plugin.Build(); err != nil {
		return err
	}
	endpoint, ok := os.LookupEnv("OLL_PLUGIN_ENDPOINT")
	if !ok {
		return errors.New("OLL_PLUGIN_ENDPOINT is required")
	}
	return plugin.runAt(ctx, endpoint, os.Stdin)
}

var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
var canonicalUUIDV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func canonicalUUIDV4(value string) bool { return canonicalUUIDV4Pattern.MatchString(value) }

func validatePluginID(value string) error {
	if len(value) > 191 {
		return errors.New("plugin ID must be at most 191 bytes")
	}
	labels := strings.Split(value, ".")
	if len(labels) < 2 {
		return errors.New("plugin ID must contain at least two DNS labels")
	}
	for _, label := range labels {
		if !dnsLabel.MatchString(label) {
			return errors.New("plugin ID must be a lower-case ASCII dotted DNS name")
		}
	}
	return nil
}

type cancellation struct {
	ctx context.Context
}

func (value cancellation) Context() context.Context { return value.ctx }
func (value cancellation) Done() <-chan struct{}    { return value.ctx.Done() }

type ActionContext interface {
	Context() context.Context
	JobID() string
	Trace() *protocol.TraceContext
	Host() *Host
	HostCall(*protocol.HostCallRequest) (*protocol.HostCallResponse, error)
	GetConfig(*protocol.ConfigPath) (*protocol.GetConfigResponse, error)
	InvokeConfigFunction(*protocol.ConfigFunctionRef, []*protocol.ConfigValue) (*protocol.InvokeConfigFunctionResponse, error)
}

type actionContext struct {
	ctx          context.Context
	jobID        string
	trace        *protocol.TraceContext
	parentCallID uint64
	host         *Host
}

func (value *actionContext) Context() context.Context      { return value.ctx }
func (value *actionContext) JobID() string                 { return value.jobID }
func (value *actionContext) Trace() *protocol.TraceContext { return cloneTrace(value.trace) }
func (value *actionContext) Host() *Host                   { return value.host }

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
	return value.host.Call(value.ctx, trace, request)
}

func (value *actionContext) GetConfig(path *protocol.ConfigPath) (*protocol.GetConfigResponse, error) {
	trace, err := value.nestedTrace()
	if err != nil {
		return nil, err
	}
	return value.host.GetConfig(value.ctx, trace, path)
}

func (value *actionContext) InvokeConfigFunction(function *protocol.ConfigFunctionRef, arguments []*protocol.ConfigValue) (*protocol.InvokeConfigFunctionResponse, error) {
	trace, err := value.nestedTrace()
	if err != nil {
		return nil, err
	}
	return value.host.InvokeConfigFunction(value.ctx, trace, function, arguments)
}

func cloneTrace(value *protocol.TraceContext) *protocol.TraceContext {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*protocol.TraceContext)
}

type activeJob struct {
	cancel context.CancelFunc
	done   <-chan struct{}
}

type jobSet struct {
	mu   sync.Mutex
	jobs map[string]activeJob
}
