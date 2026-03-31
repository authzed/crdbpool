package pool

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// CrdbRetryErrCode is the error code for transaction restart errors.
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#restart-transaction
	CrdbRetryErrCode = "40001"
	// CrdbAmbiguousErrorCode is the error code for ambiguous result errors.
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	CrdbAmbiguousErrorCode = "40003"
	// CrdbServerNotAcceptingClients is the error code when server is not accepting clients.
	// https://www.cockroachlabs.com/docs/stable/node-shutdown.html#connection-retry-loop
	CrdbServerNotAcceptingClients = "57P01"
	// CrdbUnknownSQLState is the error code when SqlState is unknown.
	CrdbUnknownSQLState = "XXUUU"
	// CrdbClockSkewMessage is the error message encountered when crdb nodes have large clock skew.
	CrdbClockSkewMessage = "cannot specify timestamp in the future"
)

// MaxRetryError is returned when the retry budget is exhausted.
type MaxRetryError struct {
	MaxRetries uint8
	LastErr    error
}

func (e *MaxRetryError) Error() string {
	if e.MaxRetries == 0 {
		if e.LastErr != nil {
			return "retries disabled: " + e.LastErr.Error()
		}
		return "retries disabled"
	}
	if e.LastErr == nil {
		return fmt.Sprintf("max retries reached (%d)", e.MaxRetries)
	}
	return fmt.Sprintf("max retries reached (%d): %s", e.MaxRetries, e.LastErr.Error())
}

func (e *MaxRetryError) Unwrap() error { return e.LastErr }

// GRPCStatus returns the gRPC status for MaxRetryError.
func (e *MaxRetryError) GRPCStatus() *status.Status {
	s, ok := status.FromError(e.Unwrap())
	if !ok {
		return nil
	}

	return s
}

// ResettableError is an error that we think may succeed if retried against a new connection.
type ResettableError struct {
	Err error
}

func (e *ResettableError) Error() string {
	if e.Err == nil {
		return "resettable error"
	}
	return "resettable error" + ": " + e.Err.Error()
}

func (e *ResettableError) Unwrap() error { return e.Err }

// GRPCStatus returns the gRPC status for ResettableError.
func (e *ResettableError) GRPCStatus() *status.Status {
	if e.Err == nil {
		return status.New(codes.Unavailable, "resettable error")
	}
	return status.New(codes.Unavailable, e.Err.Error())
}

// RetryableError is an error that can be retried against the existing connection.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable error"
	}
	return "retryable error" + ": " + e.Err.Error()
}
func (e *RetryableError) Unwrap() error { return e.Err }

// GRPCStatus returns the gRPC status for RetryableError.
func (e *RetryableError) GRPCStatus() *status.Status {
	if e.Err == nil {
		return status.New(codes.Unavailable, "retryable error")
	}
	return status.New(codes.Unavailable, e.Err.Error())
}

// sqlErrorCode attempts to extract the crdb error code from the error state.
func sqlErrorCode(err error) string {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		return ""
	}

	return pgerr.SQLState()
}
