package influxclient

import (
	"errors"
	"fmt"
)

// Error holds InfluxDB server error info
type Error struct {
	// Code of error message
	Code string
	// Error message
	Message string
	// Value of Retry-After header, if present
	RetryAfter uint
}

// NewError creates Error with simple message
func NewError(message string) *Error {
	return &Error{Message: message}
}

// Error implements Error interface
func (e *Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	} else {
		return e.Message
	}
}

// ToError return error type
func (e *Error) ToError() error {
	return errors.New(e.Error())
}
