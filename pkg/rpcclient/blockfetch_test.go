package rpcclient

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestSanitizeEndpointForDisplay(t *testing.T) {
	const raw = "HTTPS://user:password@rpc.example.com:8899/private/path?api-key=secret#fragment"
	if got, want := SanitizeEndpointForDisplay(raw), "https://rpc.example.com:8899"; got != want {
		t.Fatalf("SanitizeEndpointForDisplay() = %q, want %q", got, want)
	}
	for _, raw := range []string{"not a URL with SECRET", "ftp://user:secret@example.com/private"} {
		if got := SanitizeEndpointForDisplay(raw); got != "[configured endpoint]" {
			t.Fatalf("SanitizeEndpointForDisplay(%q) = %q", raw, got)
		}
	}
}

func TestSanitizeErrorForDisplay(t *testing.T) {
	err := errors.New("request failed: POST HTTPS://user:password@rpc.example.com:8899/private/path?api-key=QUERY_SECRET#fragment Authorization: Bearer BEARER_SECRET token=TOKEN_SECRET")
	got := SanitizeErrorForDisplay(err)
	for _, secret := range []string{"user", "password", "private", "QUERY_SECRET", "fragment", "BEARER_SECRET", "TOKEN_SECRET"} {
		if strings.Contains(got, secret) {
			t.Fatalf("secret %q leaked in %q", secret, got)
		}
	}
	if !strings.Contains(got, "request failed: POST https://rpc.example.com:8899") {
		t.Fatalf("sanitized diagnostic lost useful context: %q", got)
	}
	if got := SanitizeErrorForDisplay(nil); got != "" {
		t.Fatalf("nil error sanitized to %q", got)
	}
	wrapped := WrapErrorForDisplay(err)
	if !errors.Is(wrapped, err) {
		t.Fatal("display-safe wrapper lost the original error chain")
	}
	if got := wrapped.Error(); got != SanitizeErrorForDisplay(err) {
		t.Fatalf("display-safe error = %q, want %q", got, SanitizeErrorForDisplay(err))
	}
	if WrapErrorForDisplay(nil) != nil {
		t.Fatal("nil error wrapper is non-nil")
	}
}

func TestBlockFetch_Confirmed(t *testing.T) {
	fetcher := NewRpcClient("https://api.mainnet-beta.solana.com/")

	result, err := fetcher.GetBlockConfirmed(1234)
	assert.NoError(t, err)

	if len(result.Transactions) == 0 {
		fmt.Printf("no transactions")
	} else {
		fmt.Printf("block contained %d transactions.\n", len(result.Transactions))

		for _, tx := range result.Transactions {
			txParsed, err := tx.GetTransaction()
			assert.NoError(t, err)
			//fmt.Printf("%+v\n", txParsed)
			err = txParsed.VerifySignatures()
			assert.NoError(t, err)
		}
	}
}

func TestBlockFetch_Finalized(t *testing.T) {
	fetcher := NewRpcClient("https://api.mainnet-beta.solana.com/")

	result, err := fetcher.GetBlockFinalized(1234)
	assert.NoError(t, err)

	if len(result.Transactions) == 0 {
		fmt.Printf("no transactions")
	} else {
		fmt.Printf("block contained %d transactions.\n", len(result.Transactions))

		for _, tx := range result.Transactions {
			txParsed, err := tx.GetTransaction()
			assert.NoError(t, err)
			//fmt.Printf("%+v\n", txParsed)
			err = txParsed.VerifySignatures()
			assert.NoError(t, err)
		}
	}
}

func TestBlockFetch_LatestConfirmed(t *testing.T) {
	fetcher := NewRpcClient("https://api.mainnet-beta.solana.com/")

	result, err := fetcher.GetLatestBlockConfirmed()
	assert.NoError(t, err)

	fmt.Printf("slot: %d\n", *result.BlockHeight)

	if len(result.Transactions) == 0 {
		fmt.Printf("no transactions")
	} else {
		fmt.Printf("block contained %d transactions.\n", len(result.Transactions))

		for _, tx := range result.Transactions {
			txParsed, err := tx.GetTransaction()
			assert.NoError(t, err)
			//fmt.Printf("%+v\n", txParsed)
			err = txParsed.VerifySignatures()
			assert.NoError(t, err)
		}
	}
}

func TestBlockFetch_LatestFinalized(t *testing.T) {
	fetcher := NewRpcClient("https://api.mainnet-beta.solana.com/")

	result, err := fetcher.GetLatestBlockFinalized()
	assert.NoError(t, err)

	fmt.Printf("slot: %d\n", *result.BlockHeight)

	if len(result.Transactions) == 0 {
		fmt.Printf("no transactions")
	} else {
		fmt.Printf("block contained %d transactions.\n", len(result.Transactions))

		var numAccounts uint64
		allAccounts := make([]solana.PublicKey, 0)

		for _, tx := range result.Transactions {
			txParsed, err := tx.GetTransaction()
			assert.NoError(t, err)
			//fmt.Printf("%+v\n", txParsed)
			err = txParsed.VerifySignatures()
			assert.NoError(t, err)
			numAccounts += uint64(len(txParsed.Message.AccountKeys))
			allAccounts = append(allAccounts, txParsed.Message.AccountKeys...)
		}
		fmt.Printf("%d accounts in block\n", numAccounts)
		uniqAccts := lo.Uniq(allAccounts)
		fmt.Printf("unique accounts in block: %d\n", len(uniqAccts))
	}
}
