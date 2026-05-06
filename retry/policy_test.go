package retry_test

import (
	"net/http"
	"testing"

	"codeberg.org/audryus/resili7/retry"
)

func TestRetryPolicy(t *testing.T) {
	tests := []struct {
		err      error
		resp     *http.Response
		name     string
		expected bool
	}{
		{
			name:     "Error present",
			resp:     nil,
			err:      http.ErrHandlerTimeout,
			expected: true,
		},
		{
			name:     "Success 200",
			resp:     &http.Response{StatusCode: 200},
			err:      nil,
			expected: false,
		},
		{
			name:     "Client Error 400",
			resp:     &http.Response{StatusCode: 400},
			err:      nil,
			expected: false,
		},
		{
			name:     "Server Error 500",
			resp:     &http.Response{StatusCode: 500},
			err:      nil,
			expected: true,
		},
		{
			name:     "Rate Limit 429",
			resp:     &http.Response{StatusCode: 429},
			err:      nil,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var shouldRetry = retry.ShouldRetryDefault
			if got := shouldRetry(tt.resp, tt.err); got != tt.expected {
				t.Errorf("ShouldRetryDefault() = %v, want %v", got, tt.expected)
			}
		})
	}
}
