package node

import (
	"path/filepath"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
)

type runProgressEvents struct {
	emitter *progress.JSONLEmitter
}

func newRunProgressEvents() *runProgressEvents {
	runDir := mlog.GetLogDir()
	if runDir == "" {
		return &runProgressEvents{}
	}
	emitter, err := progress.NewJSONLEmitter(filepath.Join(runDir, progress.JSONLFileName))
	if err != nil {
		mlog.Log.Warnf("progress: failed to open JSONL event stream: %v", err)
		return &runProgressEvents{}
	}
	return &runProgressEvents{emitter: emitter}
}

func (r *runProgressEvents) Emit(phase, status, message string, fields map[string]any) {
	if r == nil || r.emitter == nil {
		return
	}
	event := map[string]any{
		"phase":   phase,
		"status":  status,
		"message": config.RedactSecretsInText(message),
	}
	for k, v := range fields {
		if s, ok := v.(string); ok {
			event[k] = config.RedactSecretsInText(s)
			continue
		}
		event[k] = v
	}
	r.emitter.Emit(event)
}

func (r *runProgressEvents) Close() {
	if r != nil && r.emitter != nil {
		r.emitter.Close()
	}
}
