package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavskye/minilogback/internal/protocol"
)

type BatchSink interface {
	WriteBatch(context.Context, [][]byte) error
}

type Config struct {
	Address        string
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	SinkTimeout    time.Duration
	DedupeEntries  int
	Limits         protocol.Limits
}

func DefaultConfig() Config {
	return Config{
		Address: "127.0.0.1:28641", MaxConnections: 256,
		ReadTimeout: 10 * time.Second, WriteTimeout: 5 * time.Second, SinkTimeout: 10 * time.Second,
		DedupeEntries: 100000, Limits: protocol.DefaultLimits(),
	}
}

func (c Config) validate() error {
	if c.Address == "" || c.MaxConnections <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 || c.SinkTimeout <= 0 || c.DedupeEntries <= 0 {
		return errors.New("collector address, positive limits, and timeouts are required")
	}
	return c.Limits.Validate()
}

type Stats struct {
	Connections      uint64
	AcceptedBatches  uint64
	DuplicateBatches uint64
	InvalidFrames    uint64
	Overloaded       uint64
	SinkErrors       uint64
}

type counters struct {
	connections      atomic.Uint64
	acceptedBatches  atomic.Uint64
	duplicateBatches atomic.Uint64
	invalidFrames    atomic.Uint64
	overloaded       atomic.Uint64
	sinkErrors       atomic.Uint64
}

type Server struct {
	config Config
	sink   BatchSink
	logger *slog.Logger
	dedupe *Dedupe
	stats  counters
	sema   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	listen net.Listener
	active map[net.Conn]struct{}
	wg     sync.WaitGroup
}

func New(config Config, sink BatchSink, logger *slog.Logger) (*Server, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errors.New("collector sink is required")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		config: config, sink: sink, logger: logger, dedupe: NewDedupe(config.DedupeEntries),
		sema: make(chan struct{}, config.MaxConnections), ctx: ctx, cancel: cancel, active: make(map[net.Conn]struct{}),
	}, nil
}

func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("listen collector: %w", err)
	}
	return s.Serve(listener)
}

func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	if s.listen != nil {
		s.mu.Unlock()
		return errors.New("collector already serving")
	}
	s.listen = listener
	s.mu.Unlock()
	if !isLoopback(listener.Addr()) {
		s.logger.Warn("collector is using trusted-network plaintext mode", "address", listener.Addr().String())
	}
	for {
		connection, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || s.ctx.Err() != nil {
				return nil
			}
			if temporary, ok := err.(interface{ Temporary() bool }); ok && temporary.Temporary() {
				continue
			}
			return fmt.Errorf("accept collector connection: %w", err)
		}
		select {
		case s.sema <- struct{}{}:
			s.mu.Lock()
			s.active[connection] = struct{}{}
			s.mu.Unlock()
			s.stats.connections.Add(1)
			s.wg.Add(1)
			go s.handle(connection)
		default:
			s.stats.overloaded.Add(1)
			_ = connection.Close()
		}
	}
}

func (s *Server) handle(connection net.Conn) {
	defer func() {
		_ = connection.Close()
		s.mu.Lock()
		delete(s.active, connection)
		s.mu.Unlock()
		<-s.sema
		s.stats.connections.Add(^uint64(0))
		s.wg.Done()
	}()
	for {
		if err := connection.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
			return
		}
		batch, err := protocol.ReadBatch(connection, s.config.Limits)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.stats.invalidFrames.Add(1)
			}
			return
		}
		key := BatchKey{ClientID: batch.Header.ClientID, BatchID: batch.Header.BatchID}
		ctx, cancel := context.WithTimeout(s.ctx, s.config.SinkTimeout)
		defer cancel()
		duplicate, sinkErr := s.dedupe.Do(ctx, key, func() error { return s.sink.WriteBatch(ctx, batch.Records) })
		status := protocol.StatusAccepted
		switch {
		case sinkErr != nil:
			s.stats.sinkErrors.Add(1)
			status = protocol.StatusSinkError
		case duplicate:
			s.stats.duplicateBatches.Add(1)
			status = protocol.StatusDuplicate
		default:
			s.stats.acceptedBatches.Add(1)
		}
		if err := connection.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
			return
		}
		if err := protocol.WriteAck(connection, protocol.Ack{Status: status, ClientID: key.ClientID, BatchID: key.BatchID}); err != nil {
			return
		}
		if sinkErr != nil {
			return
		}
	}
}

func (s *Server) Close() error {
	s.cancel()
	s.mu.Lock()
	listener := s.listen
	connections := make([]net.Conn, 0, len(s.active))
	for connection := range s.active {
		connections = append(connections, connection)
	}
	s.mu.Unlock()
	var err error
	if listener != nil {
		err = listener.Close()
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
	s.wg.Wait()
	return err
}

func (s *Server) Stats() Stats {
	return Stats{
		Connections: s.stats.connections.Load(), AcceptedBatches: s.stats.acceptedBatches.Load(),
		DuplicateBatches: s.stats.duplicateBatches.Load(), InvalidFrames: s.stats.invalidFrames.Load(),
		Overloaded: s.stats.overloaded.Load(), SinkErrors: s.stats.sinkErrors.Load(),
	}
}

func isLoopback(address net.Addr) bool {
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(stringsTrimBrackets(host))
	return ip != nil && ip.IsLoopback()
}

func stringsTrimBrackets(value string) string {
	if len(value) >= 2 && value[0] == '[' && value[len(value)-1] == ']' {
		return value[1 : len(value)-1]
	}
	return value
}
