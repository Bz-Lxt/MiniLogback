package protocol

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type DialContextFunc func(context.Context, string, string) (net.Conn, error)

type ClientConfig struct {
	Address      string
	ClientID     uint64
	DialTimeout  time.Duration
	IOTimeout    time.Duration
	RetryInitial time.Duration
	RetryMaximum time.Duration
	MaxAttempts  int
	Limits       Limits
	DialContext  DialContextFunc
}

type Client struct {
	config  ClientConfig
	mu      sync.Mutex
	conn    net.Conn
	batchID uint64
	pending *EncodedBatch
	closed  bool
}

type RemoteError struct{ Status AckStatus }

func (e *RemoteError) Error() string {
	return fmt.Sprintf("collector returned ACK status %d", e.Status)
}

func NewClient(config ClientConfig) (*Client, error) {
	if config.Address == "" {
		return nil, errors.New("collector address is required")
	}
	if config.ClientID == 0 {
		id, err := secureID()
		if err != nil {
			return nil, err
		}
		config.ClientID = id
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = 2 * time.Second
	}
	if config.IOTimeout <= 0 {
		config.IOTimeout = 5 * time.Second
	}
	if config.RetryInitial <= 0 {
		config.RetryInitial = 50 * time.Millisecond
	}
	if config.RetryMaximum <= 0 {
		config.RetryMaximum = 2 * time.Second
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 8
	}
	if config.RetryMaximum < config.RetryInitial {
		return nil, errors.New("retry maximum must be at least retry initial")
	}
	if config.Limits == (Limits{}) {
		config.Limits = DefaultLimits()
	}
	if err := config.Limits.Validate(); err != nil {
		return nil, err
	}
	if config.DialContext == nil {
		dialer := &net.Dialer{Timeout: config.DialTimeout}
		config.DialContext = dialer.DialContext
	}
	return &Client{config: config}, nil
}

func (c *Client) Send(ctx context.Context, records [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	batch := c.pending
	if batch == nil {
		c.batchID++
		if c.batchID == 0 {
			c.batchID++
		}
		var err error
		batch, err = NewEncodedBatch(c.config.ClientID, c.batchID, time.Now(), records, c.config.Limits)
		if err != nil {
			return err
		}
		c.pending = batch
	} else if !batch.Matches(records) {
		return errors.New("network retry payload changed while a batch is unacknowledged")
	}
	backoff := c.config.RetryInitial
	var lastErr error
	for attempt := 1; attempt <= c.config.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.ensureConnection(ctx); err != nil {
			lastErr = err
		} else if err := c.exchange(ctx, batch); err == nil {
			c.pending = nil
			return nil
		} else {
			var remote *RemoteError
			if errors.As(err, &remote) && remote.Status == StatusInvalid {
				return err
			}
			lastErr = err
			c.dropConnection()
		}
		if attempt < c.config.MaxAttempts {
			if err := waitContext(ctx, jitter(backoff, attempt)); err != nil {
				return err
			}
			backoff *= 2
			if backoff > c.config.RetryMaximum {
				backoff = c.config.RetryMaximum
			}
		}
	}
	return fmt.Errorf("send batch after %d attempts: %w", c.config.MaxAttempts, lastErr)
}

func (c *Client) exchange(ctx context.Context, batch *EncodedBatch) error {
	deadline := time.Now().Add(c.config.IOTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return err
	}
	if _, err := batch.WriteVectoredTo(c.conn); err != nil {
		return err
	}
	ack, err := ReadAck(c.conn)
	if err != nil {
		return err
	}
	if ack.ClientID != batch.ClientID() || ack.BatchID != batch.BatchID() {
		return fmt.Errorf("%w: ACK identity mismatch", ErrInvalidFrame)
	}
	switch ack.Status {
	case StatusAccepted, StatusDuplicate:
		return nil
	case StatusInvalid, StatusOverloaded, StatusSinkError:
		return &RemoteError{Status: ack.Status}
	default:
		return fmt.Errorf("%w: unknown ACK status", ErrInvalidFrame)
	}
}

func (c *Client) ensureConnection(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	dialCtx, cancel := context.WithTimeout(ctx, c.config.DialTimeout)
	defer cancel()
	connection, err := c.config.DialContext(dialCtx, "tcp", c.config.Address)
	if err != nil {
		return fmt.Errorf("dial collector: %w", err)
	}
	c.conn = connection
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.pending = nil
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) dropConnection() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func secureID() (uint64, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, fmt.Errorf("generate client id: %w", err)
	}
	id := binary.BigEndian.Uint64(raw[:])
	if id == 0 {
		id = 1
	}
	return id, nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func jitter(base time.Duration, attempt int) time.Duration {
	// Deterministic bounded jitter avoids a shared PRNG on the retry path.
	percent := 90 + (attempt*17)%21
	return time.Duration(int64(base) * int64(percent) / 100)
}
