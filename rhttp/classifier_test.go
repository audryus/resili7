package rhttp_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/audryus/resili7/rhttp"
)

func TestClassifyError(t *testing.T) {
	errGeneric := errors.New("generic error")

	tests := []struct {
		name     string
		resp     *http.Response
		err      error
		expected error
	}{
		{
			name:     "returns original error when transport error is present",
			resp:     nil,
			err:      errGeneric,
			expected: errGeneric,
		},
		{
			name:     "returns ErrUnknown when response is nil",
			resp:     nil,
			err:      nil,
			expected: rhttp.ErrUnknown,
		},
		{
			name: "returns ErrClientError for 408 Request Timeout",
			resp: &http.Response{
				StatusCode: http.StatusRequestTimeout,
			},
			err:      nil,
			expected: rhttp.ErrClientError,
		},
		{
			name: "returns ErrClientError for 425 Too Early",
			resp: &http.Response{
				StatusCode: http.StatusTooEarly,
			},
			err:      nil,
			expected: rhttp.ErrClientError,
		},
		{
			name: "returns ErrRateLimited for 429 Too Many Requests",
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
			},
			err:      nil,
			expected: rhttp.ErrRateLimited,
		},
		{
			name: "returns ErrServerError for 500 Internal Server Error",
			resp: &http.Response{
				StatusCode: http.StatusInternalServerError,
			},
			err:      nil,
			expected: rhttp.ErrServerError,
		},
		{
			name: "returns ErrServerError for 502 Bad Gateway",
			resp: &http.Response{
				StatusCode: http.StatusBadGateway,
			},
			err:      nil,
			expected: rhttp.ErrServerError,
		},
		{
			name: "returns ErrServerError for 503 Service Unavailable",
			resp: &http.Response{
				StatusCode: http.StatusServiceUnavailable,
			},
			err:      nil,
			expected: rhttp.ErrServerError,
		},
		{
			name: "returns ErrServerError for 504 Gateway Timeout",
			resp: &http.Response{
				StatusCode: http.StatusGatewayTimeout,
			},
			err:      nil,
			expected: rhttp.ErrServerError,
		},
		{
			name: "returns nil for 200 OK",
			resp: &http.Response{
				StatusCode: http.StatusOK,
			},
			err:      nil,
			expected: nil,
		},
		{
			name: "returns nil for 404 Not Found",
			resp: &http.Response{
				StatusCode: http.StatusNotFound,
			},
			err:      nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rhttp.ClassifyError(tt.resp, tt.err)
			if !errors.Is(result, tt.expected) {
				t.Errorf("ClassifyError() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestClassifierMiddleware(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Classifier: rhttp.NewClassifierMiddleware(),
	})

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)

	_, err := client.Do(req)
	if !errors.Is(err, rhttp.ErrServerError) {
		t.Fatalf("Expected ErrServerError, got %v", err)
	}
}

// BenchmarkHttpClassifier measures the baseline overhead of the error classifier.
// This is designed to be highly efficient and near-zero-allocation on the hot path.
//
// BenchmarkHttpClassifier/Classifier_Overhead-12         	33418423	        35.62 ns/op	       0 B/op	       0 allocs/op

func BenchmarkHttpClassifier(b *testing.B) {
	dummyResp := &http.Response{StatusCode: 200}

	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		Classifier: rhttp.NewClassifierMiddleware(),
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)

	b.Run("Classifier_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Do(req)
		}
	})
}
