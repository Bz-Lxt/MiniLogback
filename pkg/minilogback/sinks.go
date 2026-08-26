package minilogback

import (
	"io"
	"os"
	"time"

	"github.com/xavskye/minilogback/internal/protocol"
	internalsink "github.com/xavskye/minilogback/internal/sink"
)

type FileSyncPolicy string

const (
	FileSyncManual     FileSyncPolicy = "MANUAL"
	FileSyncEveryBatch FileSyncPolicy = "EVERY_BATCH"
)

type FileSinkConfig struct {
	Path           string
	MaxBytes       int64
	SyncPolicy     FileSyncPolicy
	FilePermission os.FileMode
	DirPermission  os.FileMode
}

func NewFileSink(config FileSinkConfig) (Sink, error) {
	return internalsink.NewFile(internalsink.FileConfig{
		Path:           config.Path,
		MaxBytes:       config.MaxBytes,
		SyncPolicy:     internalsink.SyncPolicy(config.SyncPolicy),
		FilePermission: config.FilePermission,
		DirPermission:  config.DirPermission,
	})
}

// NewWriterSink adapts a borrowed writer; closing the Appender does not close
// the writer itself.
func NewWriterSink(writer io.Writer) Sink { return internalsink.NewWriter(writer) }

type NetworkSinkConfig struct {
	Address                                            string
	ClientID                                           uint64
	DialTimeout, IOTimeout, RetryInitial, RetryMaximum time.Duration
	MaxAttempts                                        int
	MaxRecords, MaxPayload, MaxEventBytes              uint32
}

func NewNetworkSink(config NetworkSinkConfig) (Sink, error) {
	limits := protocol.Limits{MaxRecords: config.MaxRecords, MaxPayload: config.MaxPayload, MaxEventBytes: config.MaxEventBytes}
	if limits == (protocol.Limits{}) {
		limits = protocol.DefaultLimits()
	}
	client, err := protocol.NewClient(protocol.ClientConfig{Address: config.Address, ClientID: config.ClientID, DialTimeout: config.DialTimeout, IOTimeout: config.IOTimeout, RetryInitial: config.RetryInitial, RetryMaximum: config.RetryMaximum, MaxAttempts: config.MaxAttempts, Limits: limits})
	return internalsink.NewNetworkAdapter(client), err
}
