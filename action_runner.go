package pluginsdk

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

var errActionPanicked = errors.New("action panicked")

func runAction(sessionContext context.Context, registered action, actionContext *actionContext, arguments []string, cancel context.CancelFunc, completed chan<- jobCompletion) {
	var result ActionResult
	var actionErr error
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			logActionPanic(actionContext, recovered, debug.Stack())
			result = ActionResult{}
			actionErr = errActionPanicked
		}
		completion := jobCompletion{
			id:         actionContext.jobID,
			result:     result,
			err:        actionErr,
			contextErr: actionContext.ctx.Err(),
		}
		// Job cancellation must not discard completion: the session loop needs
		// the event to release state and send any queued cancellation replies.
		select {
		case completed <- completion:
		case <-sessionContext.Done():
		}
	}()

	result, actionErr = registered.handler(actionContext, arguments)
	if actionErr == nil {
		result, actionErr = actionContext.cloneAndValidateResult(result)
	}
}

func logActionPanic(actionContext *actionContext, recovered any, stack []byte) {
	defer func() { _ = recover() }()
	_ = actionContext.Log(
		protocol.LogLevel_LOG_LEVEL_ERROR,
		"pluginsdk.action",
		"action panicked",
		map[string]*protocol.ConfigValue{
			"job_id": stringConfigValue(actionContext.jobID),
			"panic":  stringConfigValue(fmt.Sprint(recovered)),
			"stack":  stringConfigValue(string(stack)),
		},
	)
}

func stringConfigValue(value string) *protocol.ConfigValue {
	return &protocol.ConfigValue{Kind: &protocol.ConfigValue_StringValue{StringValue: value}}
}
