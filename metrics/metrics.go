// Go port of Coda Hale's Metrics library
//
// <https://github.com/rcrowley/go-metrics>
//
// Coda Hale's original work: <https://github.com/codahale/metrics>

package metrics

var (
	metricsEnabled = false
)

// Enabled is checked by functions that are deemed 'expensive', e.g. if a
// meter-type does locking and/or non-trivial math operations during update.
func Enabled() bool {
	return metricsEnabled
}

// Enable enables the metrics system.
// The Enabled-flag is expected to be set, once, during startup, but toggling off and on
// is not supported.
//
// Enable is not safe to call concurrently. You need to call this as early as possible in
// the program, before any metrics collection will happen.
func Enable() {
	metricsEnabled = true
	startMeterTickerLoop()
}
