package node

import (
	"fmt"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"runtime"
	"strconv"
	"time"

	"github.com/Overclock-Validator/mithril/internal/mcpwire"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

const (
	mithrilEndpointHeader = mcpwire.EndpointHeader
	mithrilPprofEndpoint  = mcpwire.PprofEndpoint
)

func newPprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	mux.HandleFunc("/setblockprofilerate", func(w http.ResponseWriter, r *http.Request) {
		rateStr := r.URL.Query().Get("rate")
		rate, err := strconv.Atoi(rateStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid rate \"%s\" parameter", rateStr), http.StatusBadRequest)
			return
		}
		secondsStr := r.URL.Query().Get("seconds")
		seconds, err := strconv.Atoi(secondsStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid seconds \"%s\" parameter", secondsStr), http.StatusBadRequest)
			return
		}

		runtime.SetBlockProfileRate(rate)
		mlog.Log.Infof("Set block profile rate to %d for %d seconds", rate, seconds)
		time.AfterFunc(time.Duration(seconds)*time.Second, func() {
			runtime.SetBlockProfileRate(0)
			mlog.Log.Infof("Block profiling disabled after %d seconds", seconds)
		})

		fmt.Fprintf(w, "Block profile rate set to %d for %d seconds\n", rate, seconds)
	})

	mux.HandleFunc("/setcpuprofilerate", func(w http.ResponseWriter, r *http.Request) {
		hzStr := r.URL.Query().Get("hz")
		hz, err := strconv.Atoi(hzStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid hz \"%s\" parameter", hzStr), http.StatusBadRequest)
			return
		}

		secondsStr := r.URL.Query().Get("seconds")
		seconds, err := strconv.Atoi(secondsStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid seconds \"%s\" parameter", secondsStr), http.StatusBadRequest)
			return
		}

		runtime.SetCPUProfileRate(hz)
		mlog.Log.Infof("Set CPU profile rate to %d for %d seconds", hz, seconds)
		time.AfterFunc(time.Duration(seconds)*time.Second, func() {
			runtime.SetCPUProfileRate(0)
			mlog.Log.Infof("CPU profiling disabled after %d seconds", seconds)
		})

		fmt.Fprintf(w, "CPU profile hz set to %d for %d seconds\n", hz, seconds)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(mithrilEndpointHeader, mithrilPprofEndpoint)
		mux.ServeHTTP(w, r)
	})
}

func startPprofHandlers(pprofPort int) error {
	pprofAddr := fmt.Sprintf("127.0.0.1:%d", pprofPort)
	return startPprofHTTPServer(pprofAddr)
}

func startPprofHTTPServer(pprofAddr string) error {
	listener, err := net.Listen("tcp", pprofAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", pprofAddr, err)
	}
	mlog.Log.Infof("Starting HTTP server for pprof on %s", pprofAddr)
	go func() {
		if err := http.Serve(listener, newPprofHandler()); err != nil {
			mlog.Log.Errorf("HTTP pprof server exited: %v", err)
		}
	}()
	return nil
}
