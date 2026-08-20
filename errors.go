package pluginsdk

import (
	"errors"
	"fmt"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProtocolError reports a malformed or out-of-order session message.
type ProtocolError struct{ Message string }

// Error implements error.
func (protocolError ProtocolError) Error() string { return protocolError.Message }

// HostError reports a structured error returned by oll. Detail returns the
// complete protocol error, including metadata and typed details.
type HostError struct {
	detail *protocol.ProtocolError
}

func newHostError(detail *protocol.ProtocolError) *HostError {
	if detail == nil {
		detail = &protocol.ProtocolError{
			Code:    protocol.ErrorCode_ERROR_CODE_INTERNAL,
			Message: "host returned an empty error",
		}
	}
	return &HostError{detail: proto.Clone(detail).(*protocol.ProtocolError)}
}

// Error implements error.
func (hostError *HostError) Error() string {
	if hostError == nil || hostError.detail == nil {
		return "host rejected request"
	}
	return fmt.Sprintf("host rejected request (%s): %s", hostError.detail.Code, hostError.detail.Message)
}

// Code returns the host's typed error code.
func (hostError *HostError) Code() protocol.ErrorCode {
	if hostError == nil || hostError.detail == nil {
		return protocol.ErrorCode_ERROR_CODE_UNSPECIFIED
	}
	return hostError.detail.Code
}

// Retryable reports whether oll considers the failed operation retryable.
func (hostError *HostError) Retryable() bool {
	return hostError != nil && hostError.detail != nil && hostError.detail.Retryable
}

// Detail returns an independent copy of the complete host error.
func (hostError *HostError) Detail() *protocol.ProtocolError {
	if hostError == nil || hostError.detail == nil {
		return nil
	}
	return proto.Clone(hostError.detail).(*protocol.ProtocolError)
}

// ActionError lets an action return a structured failure to oll.
type ActionError struct {
	Code      protocol.ErrorCode
	Message   string
	Retryable bool
	Metadata  map[string]string
	Details   []*anypb.Any
}

// Error implements error.
func (actionError ActionError) Error() string {
	if actionError.Message != "" {
		return actionError.Message
	}
	return actionError.Code.String()
}

func actionProtocolError(err error) *protocol.ProtocolError {
	if err == nil {
		return nil
	}
	var pointer *ActionError
	if errors.As(err, &pointer) && pointer != nil {
		return protocolErrorFromAction(*pointer)
	}
	var value ActionError
	if errors.As(err, &value) {
		return protocolErrorFromAction(value)
	}
	var hostError *HostError
	if errors.As(err, &hostError) {
		return hostError.Detail()
	}
	return &protocol.ProtocolError{
		Code:    protocol.ErrorCode_ERROR_CODE_INTERNAL,
		Message: err.Error(),
	}
}

func protocolErrorFromAction(actionError ActionError) *protocol.ProtocolError {
	code := actionError.Code
	if code == protocol.ErrorCode_ERROR_CODE_UNSPECIFIED {
		code = protocol.ErrorCode_ERROR_CODE_INTERNAL
	}
	detail := &protocol.ProtocolError{
		Code:      code,
		Message:   actionError.Message,
		Retryable: actionError.Retryable,
		Metadata:  make(map[string]string, len(actionError.Metadata)),
		Details:   make([]*anypb.Any, 0, len(actionError.Details)),
	}
	for key, value := range actionError.Metadata {
		detail.Metadata[key] = value
	}
	for _, value := range actionError.Details {
		if value != nil {
			detail.Details = append(detail.Details, proto.Clone(value).(*anypb.Any))
		}
	}
	return detail
}
