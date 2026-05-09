package limiter

import "time"

// Option defines a functional configuration for the adaptive limiter.
type Option func(*Options)

// Options configures the BBR-inspired adaptive limiter.
// It uses fixed-point arithmetic to maintain performance in high-throughput environments.
type Options struct {
	GainCycle      []int64       // Sequence of gains used for BBR-like probing (scaled by 1024).
	InitialLimit   int64         // The concurrency limit when the limiter starts.
	MinLimit       int64         // Minimum allowed concurrency limit.
	MaxLimit       int64         // Maximum allowed concurrency limit.
	Smoothing      int64         // EMA smoothing factor (scaled by 1024).
	UpdateInterval time.Duration // Interval between limit adjustments.
}

// defaultOptions returns the recommended baseline settings for the limiter.
func defaultOptions() Options {
	return Options{
		InitialLimit:   50,
		MinLimit:       1,
		MaxLimit:       10000,
		Smoothing:      205, // Approximately 0.2 * 1024.
		UpdateInterval: 200 * time.Millisecond,

		// BBR-like probing cycle (values scaled by 1024).
		// This cycle allows the limiter to probe for higher bandwidth and drain queues.
		GainCycle: []int64{
			1280, // Probe Up (1.25 * 1024)
			1024, // Drain (1.0 * 1024)
			768,  // Probe Down (0.75 * 1024)
			1024, // Steady
			1024,
			1024,
			1024,
			1024,
		},
	}
}

// WithInitialLimit sets the starting concurrency limit.
func WithInitialLimit(v int64) Option {
	return func(o *Options) { o.InitialLimit = v }
}

// WithLimits sets the boundaries for the adaptive concurrency limit.
func WithLimits(min, max int64) Option {
	return func(o *Options) {
		o.MinLimit = min
		o.MaxLimit = max
	}
}

// WithGainCycle sets the custom BBR-like probing gain sequence.
// Values must be scaled by 1024 (e.g., 1280 = 1.25x).
func WithGainCycle(gains []int64) Option {
	return func(o *Options) { o.GainCycle = gains }
}

// WithSmoothing sets the EMA smoothing factor.
// Must be scaled by 1024 (e.g., 205 ≈ 0.2).
func WithSmoothing(v int64) Option {
	return func(o *Options) { o.Smoothing = v }
}

// WithUpdateInterval sets the interval between limit adjustments.
func WithUpdateInterval(d time.Duration) Option {
	return func(o *Options) { o.UpdateInterval = d }
}
