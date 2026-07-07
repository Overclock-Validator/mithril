package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	pb "github.com/Overclock-Validator/mithril/cmd/trace2perfetto/proto"
	exptrace "golang.org/x/exp/trace"
	"google.golang.org/protobuf/proto"
)

func main() {
	inputFile := flag.String("input", "", "input Go trace file")
	outputFile := flag.String("output", "", "output Perfetto proto file")
	startBlock := flag.Int("start-block", 0, "skip events before the Nth ProcessBlock task")
	numBlocks := flag.Int("num-blocks", 0, "convert this many blocks (0 = to the end)")
	flag.Parse()

	if *inputFile == "" {
		log.Fatal("-input is required")
	}
	if *outputFile == "" {
		log.Fatalf("-output is required")
	}

	traceFile, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("Failed to open trace file: %v", err)
	}
	defer traceFile.Close()

	r, err := exptrace.NewReader(traceFile)
	if err != nil {
		log.Fatalf("Failed to parse trace: %v", err)
	}

	trace, err := convertTrace(r, *startBlock, *numBlocks)
	if err != nil {
		log.Fatalf("Failed to convert trace: %v", err)
	}

	data, err := proto.Marshal(trace)
	if err != nil {
		log.Fatalf("Failed to marshal proto: %v", err)
	}

	err = os.WriteFile(*outputFile, data, 0644)
	if err != nil {
		log.Fatalf("Failed to write output file: %v", err)
	}
	fmt.Fprintf(os.Stderr, "Wrote %d bytes to %s\n", len(data), *outputFile)
}

type trackID uint64

// trackAllocator manages Perfetto track assignment with reuse for short-lived goroutines.
type goroutineTrackAllocator struct {
	goroutineToTrack map[exptrace.GoID]trackID
	availableTracks  []trackID
}

func newGoroutineTrackAllocator() *goroutineTrackAllocator {
	return &goroutineTrackAllocator{
		goroutineToTrack: make(map[exptrace.GoID]trackID),
		availableTracks:  nil,
	}
}

// getOrCreateTrack returns a trackID and emits a TrackDescriptor if necessary.
func (a *goroutineTrackAllocator) getOrCreateTrack(g exptrace.GoID, trace *pb.Trace) trackID {
	if t, ok := a.goroutineToTrack[g]; ok {
		return t
	}

	var t trackID
	if len(a.availableTracks) > 0 {
		t, a.availableTracks = a.availableTracks[len(a.availableTracks)-1], a.availableTracks[:len(a.availableTracks)-1]
		a.goroutineToTrack[g] = t
		return t
	}
	// Avoid 0 because protobuf
	// Avoid 1 because that's used for the process TrackDescriptor
	a.goroutineToTrack[g] = trackID(len(a.goroutineToTrack) + 2)
	trace.Packet = append(trace.Packet, &pb.TracePacket{
		TrustedPacketSequenceId: 1,
		Data: &pb.TracePacket_TrackDescriptor{
			TrackDescriptor: &pb.TrackDescriptor{
				Uuid:       uint64(a.goroutineToTrack[g]),
				Name:       fmt.Sprintf("Track %03d", a.goroutineToTrack[g]),
				ParentUuid: 1,
			},
		},
	})
	return a.goroutineToTrack[g]
}

func (a *goroutineTrackAllocator) releaseGoroutine(g exptrace.GoID) {
	if t, ok := a.goroutineToTrack[g]; ok {
		delete(a.goroutineToTrack, g)
		a.availableTracks = append(a.availableTracks, t)
	}
}

// goroutineState tracks the current non-running state slice for a goroutine
type goroutineState struct {
	state  string // "WAITING" or "RUNNABLE"
	reason string
}

// Goroutines that open one of these regions get scheduler-state
// (WAITING/RUNNABLE) slices in addition to their region slices.
var stateTrackedRegions = map[string]bool{
	"ProcessVote":        true,
	"ProcessTransaction": true,
	"Sigverify":          true,
}

func convertTrace(r *exptrace.Reader, startBlock, numBlocks int) (*pb.Trace, error) {
	trace := &pb.Trace{}

	p := uint64(1)
	trace.Packet = append(trace.Packet, &pb.TracePacket{
		TrustedPacketSequenceId: 1,
		Data: &pb.TracePacket_TrackDescriptor{
			TrackDescriptor: &pb.TrackDescriptor{
				Uuid: p,
				Name: "mithril",
				Process: &pb.ProcessDescriptor{
					Pid:         1,
					ProcessName: "mithril",
				},
			},
		},
	})

	a := newGoroutineTrackAllocator()

	waitOrSchedGoroutineStates := make(map[exptrace.GoID]*goroutineState)
	stateTracked := make(map[exptrace.GoID]bool)
	txLoopOpen := false
	blockCount := 0

	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ReadEvent: %w", err)
		}
		// Window by ProcessBlock task count. Classification still runs on
		// skipped events so goroutines entering the window keep their
		// identity; only packet emission is suppressed.
		if ev.Kind() == exptrace.EventTaskBegin && ev.Task().Type == "ProcessBlock" {
			blockCount++
		}
		if blockCount <= startBlock && startBlock > 0 {
			if ev.Kind() == exptrace.EventRegionBegin {
				if stateTrackedRegions[ev.Region().Type] {
					stateTracked[ev.Goroutine()] = true
				} else if ev.Region().Type == "TxLoop" {
					txLoopOpen = true
				}
			} else if ev.Kind() == exptrace.EventRegionEnd && ev.Region().Type == "TxLoop" {
				txLoopOpen = false
			}
			continue
		}
		if numBlocks > 0 && blockCount > startBlock+numBlocks {
			break
		}
		var eventType pb.TrackEvent_Type
		var sliceName string
		var category string
		var gid exptrace.GoID

		switch ev.Kind() {
		case exptrace.EventStateTransition:
			st := ev.StateTransition()
			if st.Resource.Kind != exptrace.ResourceGoroutine {
				continue
			}
			gid = st.Resource.Goroutine()
			_, to := st.Goroutine()

			// Release tracks for dead goroutines
			if to == exptrace.GoNotExist {
				a.releaseGoroutine(gid)
				delete(waitOrSchedGoroutineStates, gid)
				delete(stateTracked, gid)
				continue
			}
			// A sigverify goroutine is classified at birth from its start
			// frame, so its first RUNNABLE wait (the queue delay before it
			// ever runs) is captured too.
			if _, from := st.Goroutine(); from == exptrace.GoNotExist {
				for f := range st.Stack.Frames() {
					if strings.Contains(f.Func, "verifySignatures") ||
						strings.Contains(f.Func, "newSigverifyPool") {
						stateTracked[gid] = true
					}
					break
				}
			}
			if !stateTracked[gid] || !txLoopOpen {
				continue
			}

			// End any existing state slice when state changes
			if gs, ok := waitOrSchedGoroutineStates[gid]; ok {
				t := a.getOrCreateTrack(gid, trace)
				trace.Packet = append(trace.Packet, &pb.TracePacket{
					Timestamp:               uint64(ev.Time()),
					TrustedPacketSequenceId: 1,
					Data: &pb.TracePacket_TrackEvent{
						TrackEvent: &pb.TrackEvent{
							TrackUuid:  uint64(t),
							Name:       gs.state + ": " + gs.reason,
							Type:       pb.TrackEvent_TYPE_SLICE_END,
							Categories: []string{"state"},
						},
					},
				})
				delete(waitOrSchedGoroutineStates, gid)
			}

			switch to {
			case exptrace.GoWaiting:
				reason := st.Reason
				if reason == "" {
					reason = "unknown"
				}
				waitOrSchedGoroutineStates[gid] = &goroutineState{
					state:  "WAITING",
					reason: reason,
				}
				sliceName = "WAITING: " + reason

			case exptrace.GoRunnable:
				waitOrSchedGoroutineStates[gid] = &goroutineState{
					state:  "RUNNABLE",
					reason: "scheduler",
				}
				sliceName = "RUNNABLE: scheduler"

			default:
				continue
			}
			eventType = pb.TrackEvent_TYPE_SLICE_BEGIN
			category = "state"

		case exptrace.EventRegionBegin:
			gid = ev.Goroutine()
			eventType = pb.TrackEvent_TYPE_SLICE_BEGIN
			sliceName = ev.Region().Type
			category = "region"
			if stateTrackedRegions[sliceName] {
				stateTracked[gid] = true
			} else if sliceName == "TxLoop" {
				txLoopOpen = true
			}

		case exptrace.EventRegionEnd:
			gid = ev.Goroutine()
			eventType = pb.TrackEvent_TYPE_SLICE_END
			sliceName = ev.Region().Type
			category = "region"
			if sliceName == "TxLoop" {
				txLoopOpen = false
			}

		case exptrace.EventTaskBegin:
			gid = ev.Goroutine()
			eventType = pb.TrackEvent_TYPE_SLICE_BEGIN
			sliceName = ev.Task().Type
			category = "task"

		case exptrace.EventTaskEnd:
			gid = ev.Goroutine()
			eventType = pb.TrackEvent_TYPE_SLICE_END
			sliceName = ev.Task().Type
			category = "task"

		default:
			continue
		}

		t := a.getOrCreateTrack(gid, trace)

		trace.Packet = append(trace.Packet, &pb.TracePacket{
			Timestamp:               uint64(ev.Time()),
			TrustedPacketSequenceId: 1,
			Data: &pb.TracePacket_TrackEvent{
				TrackEvent: &pb.TrackEvent{
					TrackUuid:  uint64(t),
					Name:       sliceName,
					Type:       eventType,
					Categories: []string{category},
				},
			},
		})
	}

	return trace, nil
}
