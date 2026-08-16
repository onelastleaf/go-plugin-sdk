package pluginsdk

import "fmt"

type ProtocolError struct{ Message string }

func (error ProtocolError) Error() string { return error.Message }

type HostError struct {
	Code      int32
	Message   string
	Retryable bool
}

func (error HostError) Error() string {
	return fmt.Sprintf("host rejected request (%d): %s", error.Code, error.Message)
}
