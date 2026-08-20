package pluginsdk

import (
	"errors"
	"fmt"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

type shutdownSequence struct {
	replyTo      uint64
	trace        *protocol.TraceContext
	deadline     time.Time
	deadlineChan <-chan time.Time
	timer        *time.Timer
	acknowledged bool
}

func beginShutdown(request *protocol.ShutdownRequest, envelope *protocol.PluginEnvelope, jobs map[string]*activeJob) (*shutdownSequence, error) {
	if request == nil || request.GracePeriodDeadline == nil {
		return nil, ProtocolError{"ShutdownRequest requires a grace-period deadline"}
	}
	if err := request.GracePeriodDeadline.CheckValid(); err != nil {
		return nil, ProtocolError{fmt.Sprintf("shutdown deadline is invalid: %v", err)}
	}
	deadline := request.GracePeriodDeadline.AsTime()
	if !time.Now().Before(deadline) {
		return nil, errors.New("shutdown grace period has already expired")
	}
	for _, job := range jobs {
		if job.phase == jobRunning {
			job.phase = jobStopping
		}
		job.cancel()
	}
	timer := time.NewTimer(time.Until(deadline))
	return &shutdownSequence{
		replyTo:      envelope.MessageId,
		trace:        cloneTrace(envelope.Trace),
		deadline:     deadline,
		deadlineChan: timer.C,
		timer:        timer,
	}, nil
}
