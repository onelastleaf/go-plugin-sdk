package pluginsdk

import (
	"bytes"
	"errors"
	"net"
	"net/url"
	"strconv"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
)

func validateSessionReady(envelope *protocol.PluginEnvelope, expectedTrace *protocol.TraceContext) error {
	if envelope == nil || envelope.ReplyTo != nil || envelope.GetReady() == nil || !proto.Equal(envelope.Trace, expectedTrace) {
		return ProtocolError{"host SessionReady must follow PluginHello with the original trace"}
	}
	return nil
}

func validateEnvelope(envelope *protocol.PluginEnvelope, lastID *uint64, sessionID, instanceID string, maximumCallDepth, maximumCausalDepth uint32) error {
	if envelope == nil {
		return ProtocolError{"host sent an empty envelope"}
	}
	if envelope.MessageId == 0 || envelope.MessageId <= *lastID {
		return ProtocolError{"host message IDs must be nonzero and strictly increasing"}
	}
	if sessionID != "" && (envelope.SessionId != sessionID || envelope.PluginInstanceId != instanceID) {
		return ProtocolError{"host envelope belongs to another plugin instance"}
	}
	if envelope.Trace == nil || envelope.Trace.CorrelationId == "" {
		return ProtocolError{"host omitted correlation context"}
	}
	if maximumCallDepth != 0 && envelope.Trace.CallDepth > maximumCallDepth {
		return ProtocolError{"host envelope exceeds maximum call depth"}
	}
	if maximumCausalDepth != 0 && envelope.Trace.CausalDepth > maximumCausalDepth {
		return ProtocolError{"host envelope exceeds maximum causal depth"}
	}
	*lastID = envelope.MessageId
	return nil
}

func (plugin *Plugin) validateHello(hello *protocol.HostHello) error {
	if hello == nil || hello.Node == nil ||
		!proto.Equal(hello.PluginId, &protocol.PluginId{Value: plugin.id}) ||
		hello.GetPluginName().GetValue() == "" ||
		!bytes.Equal(hello.ProtocolSchemaSha256, protocolSchemaFingerprint) ||
		hello.MaximumCallDepth == 0 || hello.MaximumCausalDepth == 0 || hello.MaximumArtifactChunkBytes == 0 {
		return ProtocolError{"HostHello does not describe the expected plugin instance"}
	}
	return nil
}

func validateEndpoint(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.Opaque != "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", errors.New("OLL_PLUGIN_ENDPOINT must be an http loopback URL with an explicit port and no credentials or path")
	}
	host, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil || host == "" || portText == "" {
		return "", errors.New("OLL_PLUGIN_ENDPOINT must include a valid host and explicit port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", errors.New("OLL_PLUGIN_ENDPOINT port must be between 1 and 65535")
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", errors.New("OLL_PLUGIN_ENDPOINT must use a loopback host")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}
