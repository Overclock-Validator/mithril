package statsd

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

var statsdClient *statsd.Client

func init() {
	var err error
	statsdClient, err = statsd.New("127.0.0.1:8125")
	if err != nil {
		mlog.Log.Errorf("couldn't start statsdClient: %v", err)
	}
	statsdClient.Namespace = "mithril."

}

func Count(name string, value int64, tags []string, rate float64) error {
	if statsdClient == nil {
		return nil
	}
	return statsdClient.Count(name, value, tags, rate)
}

func Distribution(name string, value float64, tags []string, rate float64) error {
	if statsdClient == nil {
		return nil
	}
	return statsdClient.Distribution(name, value, tags, rate)
}

func Gauge(name string, value float64, tags []string, rate float64) error {
	if statsdClient == nil {
		return nil
	}
	return statsdClient.Gauge(name, value, tags, rate)
}
