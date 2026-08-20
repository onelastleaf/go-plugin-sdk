package pluginsdk

import (
	"context"
	"errors"
	"fmt"
	"io"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const maximumEnvelopeBytes = 64 * 1024 * 1024

func (plugin *Plugin) runAt(ctx context.Context, endpoint string, parent io.ReadCloser) error {
	if parent == nil {
		return errors.New("parent-liveness stream must not be nil")
	}
	target, err := validateEndpoint(endpoint)
	if err != nil {
		return err
	}
	sessionContext, cancelSession := context.WithCancel(ctx)
	defer cancelSession()

	parentEOF := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, parent)
		close(parentEOF)
	}()
	sessionDone := make(chan error, 1)
	go func() { sessionDone <- plugin.runSession(sessionContext, target) }()

	select {
	case <-parentEOF:
		cancelSession()
		_ = parent.Close()
		<-sessionDone
		return nil
	case <-ctx.Done():
		cancelSession()
		_ = parent.Close()
		<-sessionDone
		return ctx.Err()
	case sessionErr := <-sessionDone:
		cancelSession()
		_ = parent.Close()
		return sessionErr
	}
}

func (plugin *Plugin) runSession(ctx context.Context, target string) error {
	connection, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maximumEnvelopeBytes),
			grpc.MaxCallSendMsgSize(maximumEnvelopeBytes),
		),
	)
	if err != nil {
		return err
	}
	defer connection.Close()

	streamContext, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, err := protocol.NewPluginRuntimeClient(connection).Connect(streamContext)
	if err != nil {
		return err
	}
	sender := newEnvelopeSender(stream)
	lastHostID := uint64(0)
	first, err := receive(stream, &lastHostID, "", "", 0, 0)
	if err != nil {
		return err
	}
	hello := first.GetHostHello()
	if first.ReplyTo != nil || hello == nil {
		return ProtocolError{"HostHello must be the first host message"}
	}
	if first.SessionId == "" || first.PluginInstanceId == "" {
		return ProtocolError{"HostHello envelope omitted its session or instance identity"}
	}
	if err := plugin.validateHello(hello); err != nil {
		return err
	}
	if first.Trace.CallDepth > hello.MaximumCallDepth || first.Trace.CausalDepth > hello.MaximumCausalDepth {
		return ProtocolError{"HostHello exceeds a negotiated trace depth limit"}
	}
	sender.identity(first.SessionId, first.PluginInstanceId)
	actions := make([]*protocol.ActionDescriptor, 0, len(plugin.actionDescriptors))
	for _, descriptor := range plugin.actionDescriptors {
		actions = append(actions, proto.Clone(descriptor).(*protocol.ActionDescriptor))
	}
	if _, err := sender.send(nil, first.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_PluginHello{PluginHello: &protocol.PluginHello{
		PluginId:      &protocol.PluginId{Value: plugin.id},
		PluginName:    hello.PluginName,
		Actions:       actions,
		PluginVersion: plugin.version,
	}}}); err != nil {
		return err
	}
	ready, err := receive(stream, &lastHostID, first.SessionId, first.PluginInstanceId, hello.MaximumCallDepth, hello.MaximumCausalDepth)
	if err != nil {
		return err
	}
	if err := validateSessionReady(ready, first.Trace); err != nil {
		return err
	}
	if _, err := sender.send(nil, first.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Ready{Ready: &protocol.SessionReady{}}}); err != nil {
		return err
	}

	host := newHostClient(sender, hello.MaximumArtifactChunkBytes, hello.MaximumCallDepth, hello.MaximumCausalDepth)
	incoming, receiverDone := startReceiver(streamContext, stream)
	serveErr := plugin.serve(streamContext, stream, incoming, sender, host, &lastHostID)
	cancelStream()
	host.close(serveErr)
	<-receiverDone
	return serveErr
}

type receivedEnvelope struct {
	envelope *protocol.PluginEnvelope
	err      error
}

func startReceiver(ctx context.Context, stream pluginStream) (<-chan receivedEnvelope, <-chan struct{}) {
	incoming := make(chan receivedEnvelope, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			envelope, err := stream.Recv()
			select {
			case incoming <- receivedEnvelope{envelope: envelope, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return incoming, done
}

func receive(stream pluginStream, lastID *uint64, sessionID, instanceID string, maximumCallDepth, maximumCausalDepth uint32) (*protocol.PluginEnvelope, error) {
	envelope, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if err := validateEnvelope(envelope, lastID, sessionID, instanceID, maximumCallDepth, maximumCausalDepth); err != nil {
		return nil, err
	}
	return envelope, nil
}

func transportClosedError(err error) error {
	if err == io.EOF {
		return errors.New("host closed the plugin stream")
	}
	return fmt.Errorf("receive host envelope: %w", err)
}
