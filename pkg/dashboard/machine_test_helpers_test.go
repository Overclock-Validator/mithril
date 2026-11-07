package dashboard

import (
	"errors"
	"net/http"
	"sync"
)

type machineTestSink struct {
	header       http.Header
	onFirstWrite func()
	once         sync.Once
}

func (s *machineTestSink) Header() http.Header { return s.header }
func (s *machineTestSink) WriteHeader(int)     {}
func (s *machineTestSink) Write([]byte) (int, error) {
	return 0, errors.New("expected cached string writer")
}
func (s *machineTestSink) WriteString(body string) (int, error) {
	if s.onFirstWrite != nil {
		s.once.Do(s.onFirstWrite)
	}
	return len(body), nil
}
