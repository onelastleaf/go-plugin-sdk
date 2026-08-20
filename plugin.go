package pluginsdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

// ActionResult is the terminal value returned by an action. Artifacts must be
// descriptors returned by ActionContext.StoreArtifact during the same action.
type ActionResult struct {
	Result    *protocol.ConfigValue
	Artifacts []*protocol.ArtifactDescriptor
}

// StringResult constructs a successful action result containing a string.
func StringResult(value string) ActionResult {
	return ActionResult{Result: &protocol.ConfigValue{Kind: &protocol.ConfigValue_StringValue{StringValue: value}}}
}

// String returns the string value in the result, or an empty string when the
// result does not contain a string.
func (result ActionResult) String() string {
	if result.Result == nil {
		return ""
	}
	return result.Result.GetStringValue()
}

// ActionHandler handles one job. Handlers must return promptly after
// ActionContext.Context is cancelled.
type ActionHandler func(ActionContext, []string) (ActionResult, error)

type action struct {
	description string
	handler     ActionHandler
}

// Builder collects the actions exposed by a plugin. A builder is mutable and
// must not be used concurrently.
type Builder struct {
	id      string
	version string
	actions map[string]action
	err     error
}

// Plugin is an immutable, built plugin runtime. A Plugin may be run once.
type Plugin struct {
	id                string
	version           string
	actions           map[string]action
	actionDescriptors []*protocol.ActionDescriptor
	runStarted        atomic.Bool
}

// New starts building a plugin with an immutable publisher ID and an
// informational version string.
func New(id, version string) *Builder {
	return &Builder{id: id, version: version, actions: make(map[string]action)}
}

// Action registers a named action. Registration errors are returned by Build
// so calls can be chained.
func (builder *Builder) Action(name, description string, handler ActionHandler) *Builder {
	if builder.err != nil {
		return builder
	}
	if name == "" || handler == nil {
		builder.err = errors.New("action name and handler are required")
		return builder
	}
	if _, exists := builder.actions[name]; exists {
		builder.err = fmt.Errorf("duplicate action %q", name)
		return builder
	}
	builder.actions[name] = action{description: description, handler: handler}
	return builder
}

// Build validates the plugin definition and returns an immutable runtime.
func (builder *Builder) Build() (*Plugin, error) {
	if builder.err != nil {
		return nil, builder.err
	}
	if err := validatePluginID(builder.id); err != nil {
		return nil, err
	}
	if builder.version == "" {
		return nil, errors.New("plugin version must not be empty")
	}

	actions := make(map[string]action, len(builder.actions))
	names := make([]string, 0, len(builder.actions))
	for name, registered := range builder.actions {
		actions[name] = registered
		names = append(names, name)
	}
	sort.Strings(names)
	descriptors := make([]*protocol.ActionDescriptor, 0, len(names))
	for _, name := range names {
		descriptors = append(descriptors, &protocol.ActionDescriptor{
			Name:        name,
			Description: actions[name].description,
		})
	}

	return &Plugin{
		id:                builder.id,
		version:           builder.version,
		actions:           actions,
		actionDescriptors: descriptors,
	}, nil
}

// Run connects the plugin to the oll endpoint in OLL_PLUGIN_ENDPOINT and
// serves jobs until the session ends. A built plugin can only be run once.
func (plugin *Plugin) Run(ctx context.Context) error {
	if plugin == nil {
		return errors.New("plugin must not be nil")
	}
	if !plugin.runStarted.CompareAndSwap(false, true) {
		return errors.New("plugin runtime may only be run once")
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
