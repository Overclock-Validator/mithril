package node

import (
	"io"
	"os"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Replay timings follow the main log's rotation policy, independently of its
// files. json.Encoder writes each complete row in one call. Do not wrap this
// writer in bufio: a buffer flush can split a row across two rotated files.
func createReplayTimingsWriter(filename string, cfg mlog.LogConfig) (io.WriteCloser, error) {
	// Open eagerly so startup reports an unwritable destination. Append also
	// preserves any existing rows if a run's destination is reused.
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	if cfg.MaxSizeMB <= 0 {
		// Unlike lumberjack's zero value (100 MiB), the configuration's zero
		// explicitly disables size-based rotation.
		return file, nil
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    cfg.MaxSizeMB,
		MaxAge:     cfg.MaxAgeDays,
		MaxBackups: cfg.MaxBackups,
		LocalTime:  false,
		Compress:   true,
	}, nil
}
