// Package ring implements a bounded, non-blocking multiple-producer,
// single-consumer queue. The event path uses only atomic operations; it does
// not use channels, mutexes, timers, sleeps, or I/O.
package ring
