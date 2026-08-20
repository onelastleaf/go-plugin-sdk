package pluginsdk

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCompletionThenCancellationSendsTerminalBeforeIdempotentAck(t *testing.T) {
	stream := newFakePluginStream()
	sender := newEnvelopeSender(stream)
	ctx, cancel := context.WithCancel(context.Background())
	id := testJobID(1)
	jobs := map[string]*activeJob{id: {
		cancel: cancel,
		phase:  jobRunning,
		jobID:  &protocol.PluginJobId{Value: id},
		trace:  &protocol.TraceContext{CorrelationId: "job"},
	}}
	if err := completeJob(jobCompletion{id: id, result: StringResult("done")}, jobs, sender); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	terminal := nextSent(t, stream)
	if terminal.GetJobUpdate().GetState() != protocol.JobState_JOB_STATE_SUCCEEDED {
		t.Fatalf("first response = %T; want successful JobUpdate", terminal.Payload)
	}
	cancelEnvelope := &protocol.PluginEnvelope{MessageId: 99, Trace: &protocol.TraceContext{CorrelationId: "cancel"}}
	if err := cancelJob(&protocol.CancelJobRequest{JobId: &protocol.PluginJobId{Value: id}}, cancelEnvelope, jobs, sender); err != nil {
		t.Fatal(err)
	}
	ack := nextSent(t, stream)
	if ack.GetCancelJobAcknowledged().GetJobId().GetValue() != id {
		t.Fatalf("second response = %T; want cancellation acknowledgement", ack.Payload)
	}
}

func TestCancellationThenCompletionSuppressesTerminalAndAcknowledges(t *testing.T) {
	stream := newFakePluginStream()
	sender := newEnvelopeSender(stream)
	ctx, cancel := context.WithCancel(context.Background())
	id := testJobID(1)
	jobs := map[string]*activeJob{id: {
		cancel: cancel,
		phase:  jobRunning,
		jobID:  &protocol.PluginJobId{Value: id},
		trace:  &protocol.TraceContext{CorrelationId: "job"},
	}}
	cancelEnvelope := &protocol.PluginEnvelope{MessageId: 99, Trace: &protocol.TraceContext{CorrelationId: "cancel"}}
	if err := cancelJob(&protocol.CancelJobRequest{JobId: &protocol.PluginJobId{Value: id}}, cancelEnvelope, jobs, sender); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	assertNoSent(t, stream, 20*time.Millisecond)
	if err := completeJob(jobCompletion{id: id, result: StringResult("must be suppressed"), contextErr: context.Canceled}, jobs, sender); err != nil {
		t.Fatal(err)
	}
	ack := nextSent(t, stream)
	if ack.GetCancelJobAcknowledged() == nil {
		t.Fatalf("response = %T; want cancellation acknowledgement", ack.Payload)
	}
	assertNoSent(t, stream, 20*time.Millisecond)
}

func TestCancellationDoesNotBlockHeartbeatOrOtherJobs(t *testing.T) {
	slowStarted := make(chan struct{})
	slowRelease := make(chan struct{})
	plugin := mustTestPlugin(t, map[string]ActionHandler{
		"slow": func(ActionContext, []string) (ActionResult, error) {
			close(slowStarted)
			<-slowRelease
			return StringResult("late"), nil
		},
		"fast": func(ActionContext, []string) (ActionResult, error) { return StringResult("fast"), nil },
	})
	harness := newServeHarness(plugin)
	t.Cleanup(func() { _ = harness.stop() })
	slowID := testJobID(1)
	harness.send(startJobEnvelope(slowID, "slow"), nil, nil)
	if nextSent(t, harness.stream).GetJobAccepted() == nil {
		t.Fatal("slow job was not accepted")
	}
	<-slowStarted
	harness.send(&protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_CancelJob{CancelJob: &protocol.CancelJobRequest{JobId: &protocol.PluginJobId{Value: slowID}}}}, nil, nil)
	harness.send(&protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Heartbeat{Heartbeat: &protocol.Heartbeat{Nonce: 77}}}, nil, nil)
	fastID := testJobID(2)
	harness.send(startJobEnvelope(fastID, "fast"), nil, nil)

	heartbeatSeen := false
	fastAccepted := false
	fastCompleted := false
	deadline := time.After(testTimeout)
	for !(heartbeatSeen && fastAccepted && fastCompleted) {
		select {
		case envelope := <-harness.stream.sent:
			if heartbeat := envelope.GetHeartbeat(); heartbeat != nil && heartbeat.Nonce == 77 {
				heartbeatSeen = true
			}
			if accepted := envelope.GetJobAccepted(); accepted.GetJobId().GetValue() == fastID {
				fastAccepted = true
			}
			if update := envelope.GetJobUpdate(); update.GetJobId().GetValue() == fastID && update.State == protocol.JobState_JOB_STATE_SUCCEEDED {
				fastCompleted = true
			}
		case <-deadline:
			t.Fatal("cancellation stalled heartbeat or an unrelated job")
		}
	}
	close(slowRelease)
	for {
		envelope := nextSent(t, harness.stream)
		if ack := envelope.GetCancelJobAcknowledged(); ack.GetJobId().GetValue() == slowID {
			break
		}
	}
}

func TestMoreThanSixtyFourSimultaneousCompletionsAreNotDropped(t *testing.T) {
	plugin := mustTestPlugin(t, map[string]ActionHandler{
		"done": func(ActionContext, []string) (ActionResult, error) { return StringResult("done"), nil },
	})
	harness := newServeHarness(plugin)
	t.Cleanup(func() { _ = harness.stop() })
	const count = 100
	for index := 1; index <= count; index++ {
		harness.send(startJobEnvelope(testJobID(index), "done"), nil, nil)
	}
	accepted := make(map[string]bool, count)
	completed := make(map[string]bool, count)
	deadline := time.After(testTimeout)
	for len(accepted) < count || len(completed) < count {
		select {
		case envelope := <-harness.stream.sent:
			if value := envelope.GetJobAccepted(); value != nil {
				accepted[value.GetJobId().GetValue()] = true
			}
			if value := envelope.GetJobUpdate(); value != nil {
				completed[value.GetJobId().GetValue()] = true
			}
		case <-deadline:
			t.Fatalf("accepted %d and completed %d of %d jobs", len(accepted), len(completed), count)
		}
	}
}

func TestDeadlineThenCancelIsIdempotentlyAcknowledged(t *testing.T) {
	plugin := mustTestPlugin(t, map[string]ActionHandler{
		"wait": func(action ActionContext, _ []string) (ActionResult, error) {
			<-action.Context().Done()
			return ActionResult{}, nil
		},
	})
	harness := newServeHarness(plugin)
	t.Cleanup(func() { _ = harness.stop() })
	id := testJobID(1)
	start := startJobEnvelope(id, "wait")
	start.GetStartJob().Deadline = timestamppb.New(time.Now().Add(-time.Millisecond))
	harness.send(start, nil, nil)
	if nextSent(t, harness.stream).GetJobAccepted() == nil {
		t.Fatal("deadline-bound job was not accepted")
	}
	harness.send(&protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_CancelJob{CancelJob: &protocol.CancelJobRequest{JobId: &protocol.PluginJobId{Value: id}, Reason: protocol.JobCancellationReason_JOB_CANCELLATION_REASON_DEADLINE}}}, nil, nil)
	for {
		envelope := nextSent(t, harness.stream)
		if ack := envelope.GetCancelJobAcknowledged(); ack.GetJobId().GetValue() == id {
			break
		}
		if envelope.GetJobUpdate() != nil {
			t.Fatal("deadline cancellation emitted a competing terminal JobUpdate")
		}
	}
}

func TestActionContextIsCancelledAfterNormalCompletion(t *testing.T) {
	jobContext, cancel := context.WithCancel(context.Background())
	stream := newFakePluginStream()
	actionContext := newActionContext(jobContext, testJobID(1), &protocol.TraceContext{CorrelationId: "trace"}, 1, newHostClient(newEnvelopeSender(stream), 1024, 8, 8))
	completed := make(chan jobCompletion)
	go runAction(context.Background(), action{handler: func(ActionContext, []string) (ActionResult, error) {
		return ActionResult{}, nil
	}}, actionContext, nil, cancel, completed)
	<-completed
	select {
	case <-jobContext.Done():
	case <-time.After(testTimeout):
		t.Fatal("normal action completion did not release its context")
	}
}

func TestOversizedTerminalResultBecomesSmallFailure(t *testing.T) {
	stream := newFakePluginStream()
	sender := newEnvelopeSender(stream)
	job := &activeJob{jobID: &protocol.PluginJobId{Value: testJobID(1)}, trace: &protocol.TraceContext{CorrelationId: "trace"}}
	result := ActionResult{Result: stringConfigValue(strings.Repeat("x", maximumEnvelopeBytes))}
	if err := sendTerminalJobUpdate(sender, job, result, nil); err != nil {
		t.Fatal(err)
	}
	envelope := nextSent(t, stream)
	update := envelope.GetJobUpdate()
	if update == nil || update.State != protocol.JobState_JOB_STATE_FAILED || update.GetError().GetCode() != protocol.ErrorCode_ERROR_CODE_PAYLOAD_TOO_LARGE {
		t.Fatalf("fallback update = %v", update)
	}
	assertNoSent(t, stream, 20*time.Millisecond)
}

func TestActionPanicFailsOnlyThatJobAndLogsStack(t *testing.T) {
	plugin := mustTestPlugin(t, map[string]ActionHandler{
		"panic": func(ActionContext, []string) (ActionResult, error) { panic("boom") },
		"ok":    func(ActionContext, []string) (ActionResult, error) { return StringResult("ok"), nil },
	})
	harness := newServeHarness(plugin)
	t.Cleanup(func() { _ = harness.stop() })
	panicID := testJobID(1)
	okID := testJobID(2)
	harness.send(startJobEnvelope(panicID, "panic"), nil, nil)
	harness.send(startJobEnvelope(okID, "ok"), nil, nil)
	panicFailed := false
	okSucceeded := false
	stackLogged := false
	deadline := time.After(testTimeout)
	for !(panicFailed && okSucceeded && stackLogged) {
		select {
		case envelope := <-harness.stream.sent:
			if update := envelope.GetJobUpdate(); update.GetJobId().GetValue() == panicID {
				panicFailed = update.State == protocol.JobState_JOB_STATE_FAILED && update.GetError().GetCode() == protocol.ErrorCode_ERROR_CODE_INTERNAL && update.GetError().GetMessage() == "action panicked"
			}
			if update := envelope.GetJobUpdate(); update.GetJobId().GetValue() == okID {
				okSucceeded = update.State == protocol.JobState_JOB_STATE_SUCCEEDED
			}
			if record := envelope.GetLog(); record != nil {
				stackLogged = record.Fields["stack"].GetStringValue() != ""
			}
		case <-deadline:
			t.Fatalf("panicFailed=%v okSucceeded=%v stackLogged=%v", panicFailed, okSucceeded, stackLogged)
		}
	}
}

func TestShutdownWaitsForActionsBeforeAcknowledging(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	plugin := mustTestPlugin(t, map[string]ActionHandler{
		"stubborn": func(ActionContext, []string) (ActionResult, error) {
			close(started)
			<-release
			return ActionResult{}, nil
		},
	})
	harness := newServeHarness(plugin)
	id := testJobID(1)
	harness.send(startJobEnvelope(id, "stubborn"), nil, nil)
	if nextSent(t, harness.stream).GetJobAccepted() == nil {
		t.Fatal("job was not accepted")
	}
	<-started
	deadline := timestamppb.New(time.Now().Add(time.Second))
	harness.send(&protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Shutdown{Shutdown: &protocol.ShutdownRequest{GracePeriodDeadline: deadline}}}, nil, nil)
	assertNoSent(t, harness.stream, 30*time.Millisecond)
	close(release)
	ack := nextSent(t, harness.stream)
	if ack.GetShutdownAcknowledged() == nil {
		t.Fatalf("response = %T; want ShutdownAcknowledged", ack.Payload)
	}
	select {
	case <-harness.stream.closed:
	case <-time.After(testTimeout):
		t.Fatal("plugin did not half-close after acknowledging shutdown")
	}
	harness.incoming <- receivedEnvelope{err: io.EOF}
	if err := harness.wait(); err != nil {
		t.Fatalf("graceful shutdown returned %v", err)
	}
}

func TestShutdownDeadlineDoesNotProduceFalseAcknowledgement(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	plugin := mustTestPlugin(t, map[string]ActionHandler{
		"stubborn": func(ActionContext, []string) (ActionResult, error) {
			close(started)
			<-release
			return ActionResult{}, nil
		},
	})
	harness := newServeHarness(plugin)
	harness.send(startJobEnvelope(testJobID(1), "stubborn"), nil, nil)
	_ = nextSent(t, harness.stream)
	<-started
	harness.send(&protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Shutdown{Shutdown: &protocol.ShutdownRequest{GracePeriodDeadline: timestamppb.New(time.Now().Add(60 * time.Millisecond))}}}, nil, nil)
	err := harness.wait()
	close(release)
	if err == nil || !strings.Contains(err.Error(), "grace period expired") {
		t.Fatalf("shutdown error = %v; want grace-period failure", err)
	}
	assertNoSent(t, harness.stream, 20*time.Millisecond)
}

func TestShutdownDoesNotWaitForAbandonedHostResponse(t *testing.T) {
	plugin := mustTestPlugin(t, map[string]ActionHandler{
		"host": func(action ActionContext, _ []string) (ActionResult, error) {
			_, err := action.GetConfig(nil)
			return ActionResult{}, err
		},
	})
	harness := newServeHarness(plugin)
	harness.send(startJobEnvelope(testJobID(1), "host"), nil, nil)
	if nextSent(t, harness.stream).GetJobAccepted() == nil {
		t.Fatal("job was not accepted")
	}
	hostCall := nextSent(t, harness.stream)
	if hostCall.GetHostCall() == nil {
		t.Fatal("action did not start its host call")
	}
	harness.send(&protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_Shutdown{Shutdown: &protocol.ShutdownRequest{GracePeriodDeadline: timestamppb.New(time.Now().Add(time.Second))}}}, nil, nil)
	ack := nextSent(t, harness.stream)
	if ack.GetShutdownAcknowledged() == nil {
		t.Fatalf("response = %T; want ShutdownAcknowledged", ack.Payload)
	}
	if count := harness.host.activePendingCount(); count != 0 {
		t.Fatalf("active pending host calls = %d; want 0", count)
	}
	harness.send(&protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_HostResult{HostResult: &protocol.HostCallResponse{}}}, hostCall.Trace, ref(hostCall.MessageId))
	harness.incoming <- receivedEnvelope{err: io.EOF}
	if err := harness.wait(); err != nil {
		t.Fatalf("shutdown rejected the abandoned host call's late response: %v", err)
	}
	harness.host.mu.Lock()
	pending := len(harness.host.pending)
	harness.host.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending host calls after late response = %d; want 0", pending)
	}
}

func TestProtocolDeadlinesMustBeValidTimestamps(t *testing.T) {
	invalid := &timestamppb.Timestamp{Seconds: 253402300800}
	envelope := &protocol.PluginEnvelope{MessageId: 1, Trace: &protocol.TraceContext{CorrelationId: "trace"}}
	if _, err := beginShutdown(&protocol.ShutdownRequest{}, envelope, nil); err == nil {
		t.Fatal("accepted shutdown without a deadline")
	}
	if _, err := beginShutdown(&protocol.ShutdownRequest{GracePeriodDeadline: invalid}, envelope, nil); err == nil {
		t.Fatal("accepted invalid shutdown deadline")
	}

	plugin := mustTestPlugin(t, map[string]ActionHandler{
		"done": func(ActionContext, []string) (ActionResult, error) { return ActionResult{}, nil },
	})
	stream := newFakePluginStream()
	sender := newEnvelopeSender(stream)
	host := newHostClient(sender, 1024, 8, 8)
	request := startJobEnvelope(testJobID(1), "done").GetStartJob()
	request.Deadline = invalid
	if err := plugin.startJob(context.Background(), request, envelope, sender, host, make(map[string]*activeJob), make(chan jobCompletion)); err == nil {
		t.Fatal("accepted invalid job deadline")
	}
	assertNoSent(t, stream, 20*time.Millisecond)
}

func mustTestPlugin(t *testing.T, handlers map[string]ActionHandler) *Plugin {
	t.Helper()
	builder := New("dev.example.test", "0.1.0")
	for name, handler := range handlers {
		builder.Action(name, name, handler)
	}
	plugin, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return plugin
}
