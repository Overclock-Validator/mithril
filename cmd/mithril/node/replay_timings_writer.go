package node

import (
	"bufio"
	"io"
	"os"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Replay timing logs need the same bounded retention as the main log. Keep
// createBufWriter unchanged: CPU profiles must never rotate mid-profile.
func createReplayTimingsWriter(filename string) (io.Writer, func(), error) {
	cfg := mlog.DefaultConfig()
	if config.IsSet("log.max_size_mb") {
		cfg.MaxSizeMB = config.GetInt("log.max_size_mb")
	}
	if config.IsSet("log.max_backups") {
		cfg.MaxBackups = config.GetInt("log.max_backups")
	}
	if config.IsSet("log.max_age_days") {
		cfg.MaxAgeDays = config.GetInt("log.max_age_days")
	}
	return newReplayTimingsWriter(filename, cfg)
}

func newReplayTimingsWriter(filename string, cfg mlog.LogConfig) (io.Writer, func(), error) {
	// Check the destination without letting rotation rename an invalid path.
	probe, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}
	if err := probe.Close(); err != nil {
		return nil, nil, err
	}
	file := &lumberjack.Logger{
		Filename: filename, MaxSize: cfg.MaxSizeMB, MaxBackups: cfg.MaxBackups,
		MaxAge: cfg.MaxAgeDays, Compress: true,
	}
	writer := bufio.NewWriter(file)
	return writer, func() {
		if err := writer.Flush(); err != nil {
			mlog.Log.Errorf("flush replay timings: %v", err)
		}
		if err := file.Close(); err != nil {
			mlog.Log.Errorf("close replay timings: %v", err)
		}
	}, nil
}
