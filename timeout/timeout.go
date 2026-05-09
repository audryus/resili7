package timeout

import (
	"errors"
	"time"
)

var ErrTimeout = errors.New("request timeout")

// ResultAction representa uma operação que devolve um resultado tipado.
// Elimina a necessidade de ponteiros side-channel (que causam escape para o Heap).
type ResultAction[R any] interface {
	Execute() (R, error)
	Now() int64
	RequestDeadline() int64
}

func Execute[R any, A ResultAction[R]](d time.Duration, action A) (err error) {
	_, err = ExecuteWithResult(d, action)
	return err
}

// ExecuteWithResult é idêntico ao ExecuteAction mas retorna (R, error).
// Isso permite que o caller receba o resultado por valor, sem alocação no Heap.
func ExecuteWithResult[R any, A ResultAction[R]](d time.Duration, action A) (resp R, err error) {
	now := action.Now()
	requestDeadline := action.RequestDeadline()

	// Initialize the RequestDeadline if it hasn't been set by an outer middleware.
	if action.RequestDeadline() == 0 {
		requestDeadline = now + int64(d)
	}

	// Pre-execution check: fail immediately if the deadline has already passed.
	if now > requestDeadline {
		return resp, ErrTimeout
	}

	// Execute the rest of the pipeline.
	resp, err = action.Execute()

	// Post-execution check: even if the handler succeeded, if it finished after the deadline,
	// we return ErrTimeout to ensure strict timing guarantees.
	// Note: We call time.Now() here to get the most accurate finish time.
	if err == nil && time.Now().UnixNano() > requestDeadline {
		return resp, ErrTimeout
	}

	return resp, err
}
