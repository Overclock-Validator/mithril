package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// BankhashVerifier checks computed bankhashes against a reference mithril node.
// Verification is async and does not block the replay loop.
type BankhashVerifier struct {
	endpoint   string
	httpClient *http.Client
	mode       BankhashVerifyMode

	mu           sync.Mutex
	lastVerified uint64
	mismatches   int
	verified     int
	errors       int
	verifyCh     chan verifyRequest
	done         chan struct{}
}

type BankhashVerifyMode int

const (
	BankhashVerifyWarn  BankhashVerifyMode = iota // log and continue
	BankhashVerifyPanic                           // halt on mismatch
)

type verifyRequest struct {
	slot     uint64
	computed []byte
}

type jsonRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonRPCError   `json:"error"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NewBankhashVerifier(endpoint string, mode BankhashVerifyMode) *BankhashVerifier {
	v := &BankhashVerifier{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		mode:     mode,
		verifyCh: make(chan verifyRequest, 64),
		done:     make(chan struct{}),
	}
	go v.worker()
	return v
}

// Submit queues a bankhash for async verification.
func (v *BankhashVerifier) Submit(slot uint64, computedHash []byte) {
	hashCopy := make([]byte, len(computedHash))
	copy(hashCopy, computedHash)
	select {
	case v.verifyCh <- verifyRequest{slot: slot, computed: hashCopy}:
	default:
		// channel full, skip to avoid blocking replay
		v.mu.Lock()
		v.errors++
		v.mu.Unlock()
	}
}

// Stop shuts down the verification worker and logs a summary.
func (v *BankhashVerifier) Stop() {
	close(v.verifyCh)
	<-v.done
	v.mu.Lock()
	defer v.mu.Unlock()
	mlog.Log.Infof("bankhash verify shutdown: verified=%d mismatches=%d errors=%d last_slot=%d",
		v.verified, v.mismatches, v.errors, v.lastVerified)
}

func (v *BankhashVerifier) worker() {
	defer close(v.done)
	for req := range v.verifyCh {
		v.verify(req)
	}
}

func (v *BankhashVerifier) verify(req verifyRequest) {
	expected, err := v.fetchBankHash(req.slot)
	if err != nil {
		v.mu.Lock()
		v.errors++
		errCount := v.errors
		v.mu.Unlock()
		if errCount%100 == 1 {
			mlog.Log.Infof("bankhash verify: fetch error for slot %d: %v (total errors: %d)", req.slot, err, errCount)
		}
		return
	}

	v.mu.Lock()
	v.verified++
	v.lastVerified = req.slot
	v.mu.Unlock()

	if !bytes.Equal(req.computed, expected) {
		v.mu.Lock()
		v.mismatches++
		v.mu.Unlock()

		computedStr := base58.Encode(req.computed)
		expectedStr := base58.Encode(expected)

		if v.mode == BankhashVerifyPanic {
			panic(fmt.Sprintf("DIVERGENCE in slot %d: bankhash mismatch: computed=%s expected=%s", req.slot, computedStr, expectedStr))
		}
		mlog.Log.Errorf("DIVERGENCE in slot %d: bankhash mismatch: computed=%s expected=%s", req.slot, computedStr, expectedStr)
	}
}

func (v *BankhashVerifier) fetchBankHash(slot uint64) ([]byte, error) {
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "getBankHash",
		Params:  []interface{}{slot},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", v.endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := v.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(respBytes, &rpcResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	var hashStr string
	if err := json.Unmarshal(rpcResp.Result, &hashStr); err != nil {
		return nil, fmt.Errorf("unmarshal result: %w", err)
	}

	hashBytes, err := base58.DecodeFromString(hashStr)
	if err != nil {
		return nil, fmt.Errorf("decode base58 hash: %w", err)
	}

	return hashBytes[:], nil
}

// Stats returns current verification statistics.
func (v *BankhashVerifier) Stats() (verified, mismatches, errors int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.verified, v.mismatches, v.errors
}
