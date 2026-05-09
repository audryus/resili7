package limiter

import (
	"errors"
)

var ErrLimited = errors.New("rate limited")

// ResultAction representa uma operação que devolve um resultado tipado.
// Elimina a necessidade de ponteiros side-channel (que causam escape para o Heap).
type ResultAction[R any] interface {
	Execute() (R, error)
	Now() int64
}

func Execute[R any, A ResultAction[R]](l *Limiter, action A) (err error) {
	_, err = ExecuteWithResult(l, action)
	return err
}

// ExecuteWithResult é idêntico ao ExecuteAction mas retorna (R, error).
// Isso permite que o caller receba o resultado por valor, sem alocação no Heap.
func ExecuteWithResult[R any, A ResultAction[R]](l *Limiter, action A) (resp R, err error) {
	// Attempt to acquire a concurrency permit.
	// Passing req.Now to avoid an extra system time call.
	start, ok := l.Acquire(action.Now())
	if !ok {
		// Reject the request immediately if the limit is reached.
		return resp, ErrLimited
	}

	// Execute the rest of the pipeline.
	resp, err = action.Execute()

	// Report the result back to the limiter.
	// Success is defined as err == nil.
	l.Done(start, err == nil)

	return resp, err
}
