package pluginsdk

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maximumEnvelopeBytes = 64 * 1024 * 1024

func (plugin *Plugin) runAt(ctx context.Context, endpoint string, parent io.Reader) error {
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
		cancelSession()
	}()
	err = plugin.runSession(sessionContext, target)
	select {
	case <-parentEOF:
		return nil
	default:
		return err
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
	stream, err := protocol.NewPluginRuntimeClient(connection).Connect(ctx)
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
	if err := plugin.validateHello(hello); err != nil {
		return err
	}
	if first.Trace.CallDepth > hello.MaximumCallDepth || first.Trace.CausalDepth > hello.MaximumCausalDepth {
		return ProtocolError{"HostHello exceeds a negotiated trace depth limit"}
	}
	sender.identity(hello.SessionId, hello.PluginInstanceId)
	actions := make([]*protocol.ActionDescriptor, 0, len(plugin.actions))
	for name, action := range plugin.actions {
		actions = append(actions, &protocol.ActionDescriptor{Name: name, Description: action.description})
	}
	fingerprint, _ := hex.DecodeString(ProtocolSchemaSHA256)
	if _, err := sender.send(nil, first.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_PluginHello{PluginHello: &protocol.PluginHello{
		PluginId: &protocol.PluginId{Value: plugin.id}, PluginName: hello.PluginName,
		ProtocolSchemaSha256: fingerprint, Actions: actions, PluginVersion: plugin.version,
	}}}); err != nil {
		return err
	}
	ready, err := receive(stream, &lastHostID, hello.SessionId, hello.PluginInstanceId, hello.MaximumCallDepth, hello.MaximumCausalDepth)
	if err != nil {
		return err
	}
	if ready.ReplyTo != nil || ready.GetReady() == nil || ready.Trace.GetCorrelationId() != first.Trace.GetCorrelationId() {
		return ProtocolError{"host SessionReady must follow PluginHello"}
	}
	if _, err := sender.send(nil, first.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Ready{Ready: &protocol.SessionReady{}}}); err != nil {
		return err
	}

	host := newHost(sender, hello.MaximumArtifactChunkBytes, hello.MaximumCallDepth, hello.MaximumCausalDepth)
	return plugin.serve(ctx, stream, sender, host, &lastHostID)
}

func (plugin *Plugin) serve(ctx context.Context, stream protocol.PluginRuntime_ConnectClient, sender *envelopeSender, host *Host, lastHostID *uint64) error {
	type received struct {
		envelope *protocol.PluginEnvelope
		err      error
	}
	incoming := make(chan received, 1)
	go func() {
		for {
			envelope, err := stream.Recv()
			incoming <- received{envelope: envelope, err: err}
			if err != nil {
				return
			}
		}
	}()
	jobs := make(map[string]activeJob)
	jobDone := make(chan string, 64)
	for {
		select {
		case <-ctx.Done():
			cancelJobs(jobs)
			return ctx.Err()
		case id := <-jobDone:
			delete(jobs, id)
		case value := <-incoming:
			if value.err != nil {
				cancelJobs(jobs)
				return value.err
			}
			envelope := value.envelope
			pruneFinishedJobs(jobs)
			if err := validateEnvelope(envelope, lastHostID, sender.sessionID, sender.instanceID, host.maximumCallDepth, host.maximumCausalDepth); err != nil {
				cancelJobs(jobs)
				return err
			}
			if envelope.ReplyTo != nil {
				if err := host.route(envelope); err != nil {
					return err
				}
				continue
			}
			switch payload := envelope.Payload.(type) {
			case *protocol.PluginEnvelope_StartJob:
				if err := plugin.startJob(ctx, payload.StartJob, envelope, sender, host, jobs, jobDone); err != nil {
					return err
				}
			case *protocol.PluginEnvelope_CancelJob:
				id := payload.CancelJob.GetJobId().GetValue()
				job, ok := jobs[id]
				if !ok {
					return ProtocolError{"cancellation names no active job"}
				}
				job.cancel()
				<-job.done
				delete(jobs, id)
				_, err := sender.send(ref(envelope.MessageId), envelope.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_CancelJobAcknowledged{CancelJobAcknowledged: &protocol.CancelJobAcknowledged{JobId: payload.CancelJob.JobId}}})
				if err != nil {
					return err
				}
			case *protocol.PluginEnvelope_Heartbeat:
				if _, err := sender.send(ref(envelope.MessageId), envelope.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Heartbeat{Heartbeat: payload.Heartbeat}}); err != nil {
					return err
				}
			case *protocol.PluginEnvelope_Shutdown:
				cancelJobs(jobs)
				if _, err := sender.send(ref(envelope.MessageId), envelope.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ShutdownAcknowledged{ShutdownAcknowledged: &protocol.ShutdownAcknowledged{}}}); err != nil {
					return err
				}
				return nil
			case *protocol.PluginEnvelope_ProtocolError:
				return HostError{Code: int32(payload.ProtocolError.Code), Message: payload.ProtocolError.Message, Retryable: payload.ProtocolError.Retryable}
			default:
				return ProtocolError{"unexpected host-initiated message"}
			}
		}
	}
}

func (plugin *Plugin) startJob(sessionContext context.Context, request *protocol.StartJobRequest, envelope *protocol.PluginEnvelope, sender *envelopeSender, host *Host, jobs map[string]activeJob, jobDone chan<- string) error {
	id := request.GetJobId().GetValue()
	if !canonicalUUIDV4(id) {
		return ProtocolError{"job ID must be a canonical UUID v4"}
	}
	if _, exists := jobs[id]; exists {
		return ProtocolError{"duplicate active job ID"}
	}
	invocation := request.GetAction()
	if invocation == nil {
		return ProtocolError{"unsupported job invocation"}
	}
	action, ok := plugin.actions[invocation.Action]
	if !ok {
		return ProtocolError{fmt.Sprintf("unknown action %q", invocation.Action)}
	}
	if _, err := sender.send(ref(envelope.MessageId), envelope.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_JobAccepted{JobAccepted: &protocol.JobAccepted{JobId: request.JobId}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(sessionContext)
	if request.Deadline != nil {
		cancel()
		ctx, cancel = context.WithDeadline(sessionContext, request.Deadline.AsTime())
	}
	done := make(chan struct{})
	jobs[id] = activeJob{cancel: cancel, done: done}
	go func() {
		defer close(done)
		defer func() {
			select {
			case jobDone <- id:
			default:
			}
		}()
		result, err := action.handler(&actionContext{
			ctx:          ctx,
			jobID:        id,
			trace:        cloneTrace(envelope.Trace),
			parentCallID: envelope.MessageId,
			host:         host,
		}, invocation.Arguments)
		if ctx.Err() != nil {
			return
		}
		update := &protocol.JobUpdate{JobId: request.JobId, State: protocol.JobState_JOB_STATE_SUCCEEDED, Progress: ref(float64(1)), Result: result.Result, Artifacts: result.Artifacts}
		if err != nil {
			update.State = protocol.JobState_JOB_STATE_FAILED
			update.Result = nil
			update.Artifacts = nil
			update.Error = &protocol.ProtocolError{Code: protocol.ErrorCode_ERROR_CODE_INTERNAL, Message: err.Error()}
		}
		_, _ = sender.send(nil, envelope.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_JobUpdate{JobUpdate: update}})
	}()
	return nil
}

func cancelJobs(jobs map[string]activeJob) {
	for _, job := range jobs {
		job.cancel()
	}
}

func pruneFinishedJobs(jobs map[string]activeJob) {
	for id, job := range jobs {
		select {
		case <-job.done:
			delete(jobs, id)
		default:
		}
	}
}

func receive(stream protocol.PluginRuntime_ConnectClient, lastID *uint64, sessionID, instanceID string, maximumCallDepth, maximumCausalDepth uint32) (*protocol.PluginEnvelope, error) {
	envelope, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if err := validateEnvelope(envelope, lastID, sessionID, instanceID, maximumCallDepth, maximumCausalDepth); err != nil {
		return nil, err
	}
	return envelope, nil
}

func validateEnvelope(envelope *protocol.PluginEnvelope, lastID *uint64, sessionID, instanceID string, maximumCallDepth, maximumCausalDepth uint32) error {
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
	fingerprint, _ := hex.DecodeString(ProtocolSchemaSHA256)
	if hello.Node == nil || hello.SessionId == "" || hello.PluginInstanceId == "" || !bytes.Equal(hello.ProtocolSchemaSha256, fingerprint) || hello.GetPluginId().GetValue() != plugin.id || hello.GetPluginName().GetValue() == "" || hello.MaximumCallDepth == 0 || hello.MaximumCausalDepth == 0 || hello.MaximumArtifactChunkBytes == 0 {
		return ProtocolError{"HostHello does not describe the expected plugin instance"}
	}
	return nil
}

func validateEndpoint(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Port() == "" {
		return "", errors.New("OLL_PLUGIN_ENDPOINT must be an http loopback URL with an explicit port")
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", errors.New("OLL_PLUGIN_ENDPOINT must use a loopback host")
	}
	return parsed.Host, nil
}
