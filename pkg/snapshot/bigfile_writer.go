package snapshot

import (
	"fmt"
	"os"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/panjf2000/ants/v2"
)

// writeTask is handed to the async writer goroutine: write buf[:writeLen] to the
// big file sequentially, then dispatch the batched index-builder task. The
// builder returns buf to bufPool when done (bufPool is nil for one-off buffers).
type writeTask struct {
	buf        []byte
	writeLen   int
	entries    []appendVecEntry
	baseOffset uint64 // global big-file offset of entries[0]
	fileId     uint64
	bufPool    chan []byte
}

// bigFileWriter drains writeTasks on its own goroutine: it writes each buffer to
// the big file sequentially, then hands the batch to the index-builder pool
// (which returns the buffer to its pool). Decoupling the writes from
// decompression lets disk I/O and the next read overlap.
//
// Errors here are unrecoverable during bootstrap, so the writer panics rather
// than threading an error back — matching the old appendvec copying pool, which
// also panicked. This keeps the reader-side control flow free of writer-error
// plumbing.
type bigFileWriter struct {
	fd          *os.File
	builderPool *ants.PoolWithFunc
	wg          *sync.WaitGroup
	ch          chan writeTask
	done        chan struct{}
	writers     int // concurrent pwrites per buffer (queue depth to the RAID)
}

// newBigFileWriter starts the writer goroutine. depth bounds the number of
// buffers that may be queued for writing before submit blocks. writers is the
// number of concurrent pwrites each buffer is split into.
func newBigFileWriter(fd *os.File, builderPool *ants.PoolWithFunc, wg *sync.WaitGroup, depth, writers int) *bigFileWriter {
	if writers < 1 {
		writers = 1
	}
	w := &bigFileWriter{
		fd:          fd,
		builderPool: builderPool,
		wg:          wg,
		ch:          make(chan writeTask, depth),
		done:        make(chan struct{}),
		writers:     writers,
	}
	go w.run()
	return w
}

func (w *bigFileWriter) submit(task writeTask) { w.ch <- task }

// close stops accepting tasks and waits for the writer to finish. Once it
// returns, every builder task has been dispatched (its wg.Add already called),
// so the caller may safely wg.Wait.
func (w *bigFileWriter) close() {
	close(w.ch)
	<-w.done
}

func (w *bigFileWriter) run() {
	defer close(w.done)
	var fileOffset int64
	for task := range w.ch {
		if task.writeLen > 0 {
			w.writeParallel(task.buf[:task.writeLen], fileOffset)
			fileOffset += int64(task.writeLen)
		}
		if len(task.entries) > 0 {
			w.wg.Add(1)
			if err := w.builderPool.Invoke(indexEntryBuilderTask{
				Entries:    task.entries,
				BaseOffset: task.baseOffset,
				FileId:     task.fileId,
				Buf:        task.buf,
				Pool:       task.bufPool,
			}); err != nil {
				panic(fmt.Sprintf("indexEntryBuilderPool.Invoke: %v", err))
			}
		} else if task.bufPool != nil {
			task.bufPool <- task.buf
		}
	}
}

// writeParallel splits buf into up to w.writers page-aligned pieces and writes
// them concurrently at off, raising queue depth to the RAID. buf's length is
// always page-aligned, so every piece is too (required for O_DIRECT).
func (w *bigFileWriter) writeParallel(buf []byte, off int64) {
	chunk := len(buf) / w.writers
	chunk -= chunk % accountsdb.PageSize
	if w.writers <= 1 || chunk == 0 {
		if _, err := w.fd.WriteAt(buf, off); err != nil {
			panic(fmt.Sprintf("snapshot big-file WriteAt offset=%d len=%d: %v", off, len(buf), err))
		}
		return
	}
	var wg sync.WaitGroup
	for start := 0; start < len(buf); start += chunk {
		end := start + chunk
		if end > len(buf) {
			end = len(buf)
		}
		wg.Add(1)
		go func(p []byte, at int64) {
			defer wg.Done()
			if _, err := w.fd.WriteAt(p, at); err != nil {
				panic(fmt.Sprintf("snapshot big-file WriteAt offset=%d len=%d: %v", at, len(p), err))
			}
		}(buf[start:end], off+int64(start))
	}
	wg.Wait()
}
