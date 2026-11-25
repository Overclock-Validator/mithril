package accountsdb

import "os"

// MITHRIL_BATCH_CACHE_PROBE_SERIAL=1 keeps a batch's cache probes on the
// calling goroutine (the behavior before they were split across workers).
var batchCacheProbeSerial = os.Getenv("MITHRIL_BATCH_CACHE_PROBE_SERIAL") == "1"
