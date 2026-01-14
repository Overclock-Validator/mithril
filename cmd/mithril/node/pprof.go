package node

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/* handlers
	"runtime"
	"strconv"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

func startPprofHandlers(pprofPort int) {
	pprofAddr := fmt.Sprintf("localhost:%d", pprofPort)
	mlog.Log.Infof("Starting HTTP server for pprof on %s", pprofAddr)
	go func() {

		http.HandleFunc("/setblockprofilerate", func(w http.ResponseWriter, r *http.Request) {
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

		http.HandleFunc("/setcpuprofilerate", func(w http.ResponseWriter, r *http.Request) {
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

			// Respond to the client
			fmt.Fprintf(w, "CPU profile hz set to %d for %d seconds\n", hz, seconds)
		})

		mlog.Log.Errorf("HTTP pprof server exited: %v", http.ListenAndServe(pprofAddr, nil))
	}()
}
