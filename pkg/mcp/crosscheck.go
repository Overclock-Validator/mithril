package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// `confirmed` is the default commitment: close to the tip but without the
// dropped-fork risk of `processed`.
const defaultCommitment = "confirmed"

const (
	referenceRPCAttempts   = 3
	referenceRPCRetryDelay = 750 * time.Millisecond
)

func validateCommitment(c string) error {
	switch c {
	case "processed", "confirmed", "finalized":
		return nil
	default:
		return fmt.Errorf("invalid commitment; must be one of processed, confirmed, finalized")
	}
}

// ReferenceEpochInfo is the subset of getEpochInfo needed for slots-behind.
type ReferenceEpochInfo struct {
	AbsoluteSlot uint64 `json:"absoluteSlot"`
	BlockHeight  uint64 `json:"blockHeight"`
	Epoch        uint64 `json:"epoch"`
}

// getEpochInfoAt calls getEpochInfo at a commitment (used for the trusted
// reference RPC). Reuses the SSRF-guarded, capped JSON-RPC client.
func (c *mithrilRPCClient) getEpochInfoAt(ctx context.Context, commitment string) (ReferenceEpochInfo, error) {
	raw, err := c.call(ctx, "getEpochInfo", []any{map[string]string{"commitment": commitment}})
	if err != nil {
		return ReferenceEpochInfo{}, err
	}
	var decoded struct {
		AbsoluteSlot *uint64 `json:"absoluteSlot"`
		BlockHeight  *uint64 `json:"blockHeight"`
		Epoch        *uint64 `json:"epoch"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return ReferenceEpochInfo{}, fmt.Errorf("failed to parse getEpochInfo response")
	}
	if decoded.AbsoluteSlot == nil || decoded.BlockHeight == nil || decoded.Epoch == nil {
		return ReferenceEpochInfo{}, fmt.Errorf("getEpochInfo response is missing required fields")
	}
	return ReferenceEpochInfo{AbsoluteSlot: *decoded.AbsoluteSlot, BlockHeight: *decoded.BlockHeight, Epoch: *decoded.Epoch}, nil
}

func getReferenceEpochInfo(
	ctx context.Context,
	client *mithrilRPCClient,
	commitment string,
	attempts int,
	retryDelay time.Duration,
) (ReferenceEpochInfo, error) {
	if attempts < 1 {
		return ReferenceEpochInfo{}, fmt.Errorf("reference RPC attempts must be positive")
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		info, err := client.getEpochInfoAt(ctx, commitment)
		if err == nil {
			return info, nil
		}
		lastErr = err
		if !transientReferenceRPCError(err) {
			return ReferenceEpochInfo{}, err
		}
		if attempt+1 == attempts {
			break
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ReferenceEpochInfo{}, ctx.Err()
		case <-timer.C:
		}
	}
	return ReferenceEpochInfo{}, lastErr
}

func transientReferenceRPCError(err error) bool {
	var status *rpcHTTPStatusError
	if errors.As(err, &status) {
		return status.code == http.StatusTooManyRequests || status.code >= http.StatusInternalServerError
	}
	var networkErr net.Error
	return errors.As(err, &networkErr)
}

// SlotComparison is the result of a slots-behind comparison.
type SlotComparison struct {
	MithrilSlot   uint64 `json:"mithril_slot"`
	ReferenceSlot uint64 `json:"reference_slot"`
	// SlotsBehind = reference - Mithril. Positive means behind; negative means
	// Mithril reports a higher slot, usually from commitment skew.
	SlotsBehind         int64  `json:"slots_behind"`
	ReferenceCommitment string `json:"reference_commitment"`
	MithrilView         string `json:"mithril_view"`
	Threshold           uint64 `json:"threshold"`
	Status              string `json:"status"` // in_sync | behind | ahead
}

// compareSlots preserves uint64 ordering and clamps only the signed display
// field when the exact difference cannot fit in int64.
func compareSlots(mithrilSlot, referenceSlot, threshold uint64, commitment string) SlotComparison {
	var slotsBehind int64
	if referenceSlot >= mithrilSlot {
		if d := referenceSlot - mithrilSlot; d > math.MaxInt64 {
			slotsBehind = math.MaxInt64
		} else {
			slotsBehind = int64(d)
		}
	} else {
		if d := mithrilSlot - referenceSlot; d > math.MaxInt64 {
			slotsBehind = math.MinInt64
		} else {
			slotsBehind = -int64(d)
		}
	}

	status := "in_sync"
	switch {
	case referenceSlot > mithrilSlot && referenceSlot-mithrilSlot > threshold:
		status = "behind"
	case mithrilSlot > referenceSlot:
		status = "ahead"
	}

	return SlotComparison{
		MithrilSlot:         mithrilSlot,
		ReferenceSlot:       referenceSlot,
		SlotsBehind:         slotsBehind,
		ReferenceCommitment: commitment,
		MithrilView:         "local_unfinalized_head",
		Threshold:           threshold,
		Status:              status,
	}
}

// slotsBehindCheck fetches Mithril's slot and the reference RPC's slot and
// compares them. Shared by the cross_check tool and diagnose.
func slotsBehindCheck(ctx context.Context, cfg Config, referenceURL, commitment string) (SlotComparison, error) {
	mithrilClient, err := newMithrilRPCClient(cfg.RPCURL)
	if err != nil {
		return SlotComparison{}, fmt.Errorf("mithril RPC client init failed: %w", err)
	}
	refClient, err := newMithrilRPCClient(referenceURL)
	if err != nil {
		return SlotComparison{}, err
	}
	refInfo, err := getReferenceEpochInfo(
		ctx,
		refClient,
		commitment,
		referenceRPCAttempts,
		referenceRPCRetryDelay,
	)
	if err != nil {
		return SlotComparison{}, err
	}
	mithrilInfo, err := mithrilClient.getSlotInfo(ctx)
	if err != nil {
		return SlotComparison{}, fmt.Errorf("reading Mithril slot failed: %w", err)
	}
	return compareSlots(mithrilInfo.AbsoluteSlot, refInfo.AbsoluteSlot, cfg.SlotsBehindWarn, commitment), nil
}

type crossCheckInput struct {
	Commitment string `json:"commitment,omitempty" jsonschema:"reference RPC commitment: processed, confirmed (default), or finalized"`
}

func registerCrossCheckTool(server *mcpsdk.Server, cfg Config) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_cross_check_slot",
		Annotations: annReadOnlyNetwork,
		Description: "Compare Mithril's local replay slot with a trusted reference RPC. Returns both views, slot difference, threshold, and status; commitment skew can be normal.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in crossCheckInput) (*mcpsdk.CallToolResult, SlotComparison, error) {
		referenceURL := cfg.ReferenceRPCURL
		if referenceURL == "" {
			return nil, SlotComparison{}, fmt.Errorf("MITHRIL_REFERENCE_RPC_URL is not configured")
		}
		commitment := in.Commitment
		if commitment == "" {
			commitment = defaultCommitment
		}
		if err := validateCommitment(commitment); err != nil {
			return nil, SlotComparison{}, err
		}
		cmp, err := slotsBehindCheck(ctx, cfg, referenceURL, commitment)
		if err != nil {
			safe := sanitizeURLForDisplay(referenceURL)
			return nil, SlotComparison{}, fmt.Errorf("slot cross-check against %s failed: %w", safe, err)
		}
		return nil, cmp, nil
	})
}
