package pluginsdk

import (
	"context"
	"errors"
	"fmt"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
)

type jobPhase uint8

const (
	jobRunning jobPhase = iota
	jobCancelling
	jobStopping
)

type cancellationReply struct {
	replyTo uint64
	trace   *protocol.TraceContext
	jobID   *protocol.PluginJobId
}

type activeJob struct {
	cancel              context.CancelFunc
	phase               jobPhase
	jobID               *protocol.PluginJobId
	trace               *protocol.TraceContext
	cancellationReplies []cancellationReply
}

type jobCompletion struct {
	id         string
	result     ActionResult
	err        error
	contextErr error
}

func (plugin *Plugin) serve(
	ctx context.Context,
	stream pluginStream,
	incoming <-chan receivedEnvelope,
	sender *envelopeSender,
	host *hostClient,
	lastHostID *uint64,
) error {
	// This loop is the sole owner of job phases. Workers only publish completion
	// events, which makes terminal-update/cancellation ordering deterministic.
	jobs := make(map[string]*activeJob)
	completed := make(chan jobCompletion)
	var shutdown *shutdownSequence
	defer func() {
		if shutdown != nil && shutdown.timer != nil {
			shutdown.timer.Stop()
		}
		for _, job := range jobs {
			job.cancel()
		}
	}()

	for {
		// A shutdown acknowledgement is truthful only after every handler and
		// in-flight host response has settled. oll enforces the deadline if a
		// handler ignores its cancelled context.
		if shutdown != nil && !shutdown.acknowledged && len(jobs) == 0 && host.activePendingCount() == 0 {
			if !time.Now().Before(shutdown.deadline) {
				return errors.New("shutdown grace period expired before actions settled")
			}
			if _, err := sender.send(ref(shutdown.replyTo), shutdown.trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ShutdownAcknowledged{ShutdownAcknowledged: &protocol.ShutdownAcknowledged{}}}); err != nil {
				return err
			}
			shutdown.acknowledged = true
			_ = stream.CloseSend()
		}

		var shutdownDeadline <-chan time.Time
		if shutdown != nil {
			shutdownDeadline = shutdown.deadlineChan
		}
		select {
		case <-ctx.Done():
			if shutdown != nil && shutdown.acknowledged {
				return nil
			}
			return ctx.Err()
		case err := <-sender.failure():
			if shutdown != nil && shutdown.acknowledged {
				return nil
			}
			return err
		case <-host.changeEvents():
			continue
		case <-shutdownDeadline:
			if shutdown.acknowledged {
				return nil
			}
			return errors.New("shutdown grace period expired before actions settled")
		case completion := <-completed:
			if err := completeJob(completion, jobs, sender); err != nil {
				return err
			}
		case value := <-incoming:
			if value.err != nil {
				if shutdown != nil && shutdown.acknowledged {
					return nil
				}
				return transportClosedError(value.err)
			}
			envelope := value.envelope
			if err := validateEnvelope(envelope, lastHostID, sender.sessionID, sender.instanceID, host.maximumCallDepth, host.maximumCausalDepth); err != nil {
				return err
			}
			if envelope.ReplyTo != nil {
				if err := host.route(envelope); err != nil {
					return err
				}
				continue
			}
			if shutdown != nil && shutdown.acknowledged {
				return ProtocolError{"host sent a message after ShutdownRequest was acknowledged"}
			}
			switch payload := envelope.Payload.(type) {
			case *protocol.PluginEnvelope_StartJob:
				if shutdown != nil {
					return ProtocolError{"host started a job after requesting shutdown"}
				}
				if err := plugin.startJob(ctx, payload.StartJob, envelope, sender, host, jobs, completed); err != nil {
					return err
				}
			case *protocol.PluginEnvelope_CancelJob:
				if err := cancelJob(payload.CancelJob, envelope, jobs, sender); err != nil {
					return err
				}
			case *protocol.PluginEnvelope_Heartbeat:
				if payload.Heartbeat == nil {
					return ProtocolError{"heartbeat payload is required"}
				}
				if _, err := sender.send(ref(envelope.MessageId), envelope.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Heartbeat{Heartbeat: payload.Heartbeat}}); err != nil {
					return err
				}
			case *protocol.PluginEnvelope_Shutdown:
				if shutdown != nil {
					return ProtocolError{"host sent more than one ShutdownRequest"}
				}
				sequence, err := beginShutdown(payload.Shutdown, envelope, jobs)
				if err != nil {
					return err
				}
				shutdown = sequence
			case *protocol.PluginEnvelope_ProtocolError:
				return newHostError(payload.ProtocolError)
			default:
				return ProtocolError{"unexpected host-initiated message"}
			}
		}
	}
}

func (plugin *Plugin) startJob(
	sessionContext context.Context,
	request *protocol.StartJobRequest,
	envelope *protocol.PluginEnvelope,
	sender *envelopeSender,
	host *hostClient,
	jobs map[string]*activeJob,
	completed chan<- jobCompletion,
) error {
	if request == nil {
		return ProtocolError{"StartJobRequest payload is required"}
	}
	id := request.GetJobId().GetValue()
	if !canonicalUUIDV4(id) {
		return ProtocolError{"job ID must be a canonical UUID v4"}
	}
	if _, exists := jobs[id]; exists {
		return ProtocolError{"duplicate active job ID"}
	}
	if request.Deadline != nil {
		if err := request.Deadline.CheckValid(); err != nil {
			return ProtocolError{fmt.Sprintf("job deadline is invalid: %v", err)}
		}
	}
	invocation := request.GetAction()
	if invocation == nil {
		return ProtocolError{"unsupported job invocation"}
	}
	registered, ok := plugin.actions[invocation.Action]
	if !ok {
		return ProtocolError{fmt.Sprintf("unknown action %q", invocation.Action)}
	}
	if _, err := sender.send(ref(envelope.MessageId), envelope.Trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_JobAccepted{JobAccepted: &protocol.JobAccepted{JobId: request.JobId}}}); err != nil {
		return err
	}

	var jobContext context.Context
	var cancel context.CancelFunc
	if request.Deadline == nil {
		jobContext, cancel = context.WithCancel(sessionContext)
	} else {
		jobContext, cancel = context.WithDeadline(sessionContext, request.Deadline.AsTime())
	}
	job := &activeJob{
		cancel: cancel,
		phase:  jobRunning,
		jobID:  proto.Clone(request.JobId).(*protocol.PluginJobId),
		trace:  cloneTrace(envelope.Trace),
	}
	jobs[id] = job
	actionContext := newActionContext(jobContext, id, envelope.Trace, envelope.MessageId, host)
	arguments := append([]string(nil), invocation.Arguments...)
	go runAction(sessionContext, registered, actionContext, arguments, cancel, completed)
	return nil
}

func completeJob(completion jobCompletion, jobs map[string]*activeJob, sender *envelopeSender) error {
	job, exists := jobs[completion.id]
	if !exists {
		return ProtocolError{"action completion names no active job"}
	}
	delete(jobs, completion.id)
	job.cancel()
	switch job.phase {
	case jobCancelling:
		for _, reply := range job.cancellationReplies {
			if err := sendCancellationAcknowledgement(sender, reply); err != nil {
				return err
			}
		}
		return nil
	case jobStopping:
		return nil
	case jobRunning:
		if completion.contextErr != nil {
			return nil
		}
		return sendTerminalJobUpdate(sender, job, completion.result, completion.err)
	default:
		return errors.New("invalid internal job phase")
	}
}

func sendTerminalJobUpdate(sender *envelopeSender, job *activeJob, result ActionResult, actionErr error) error {
	update := &protocol.JobUpdate{
		JobId:     job.jobID,
		State:     protocol.JobState_JOB_STATE_SUCCEEDED,
		Progress:  ref(float64(1)),
		Result:    result.Result,
		Artifacts: result.Artifacts,
	}
	if actionErr != nil {
		update.State = protocol.JobState_JOB_STATE_FAILED
		update.Result = nil
		update.Artifacts = nil
		update.Error = actionProtocolError(actionErr)
	}
	_, err := sender.send(nil, job.trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_JobUpdate{JobUpdate: update}})
	return err
}

func cancelJob(request *protocol.CancelJobRequest, envelope *protocol.PluginEnvelope, jobs map[string]*activeJob, sender *envelopeSender) error {
	if request == nil {
		return ProtocolError{"CancelJobRequest payload is required"}
	}
	id := request.GetJobId().GetValue()
	if !canonicalUUIDV4(id) {
		return ProtocolError{"cancelled job ID must be a canonical UUID v4"}
	}
	reply := cancellationReply{
		replyTo: envelope.MessageId,
		trace:   cloneTrace(envelope.Trace),
		jobID:   proto.Clone(request.JobId).(*protocol.PluginJobId),
	}
	job, exists := jobs[id]
	if !exists {
		return sendCancellationAcknowledgement(sender, reply)
	}
	job.cancellationReplies = append(job.cancellationReplies, reply)
	if job.phase != jobCancelling {
		job.phase = jobCancelling
		job.cancel()
	}
	return nil
}

func sendCancellationAcknowledgement(sender *envelopeSender, reply cancellationReply) error {
	_, err := sender.send(ref(reply.replyTo), reply.trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_CancelJobAcknowledged{CancelJobAcknowledged: &protocol.CancelJobAcknowledged{JobId: reply.jobID}}})
	return err
}
