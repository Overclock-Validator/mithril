package gossip

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"testing"
)

func TestClientAdvertisesSwappedIdentity(t *testing.T) {
	_, oldKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, newKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	holder := identityHolder(t, oldKey)
	client, err := NewClient(Config{
		Entrypoint:   "127.0.0.1:8000",
		BindAddr:     "0.0.0.0:0",
		TVUAddr:      "0.0.0.0:8001",
		AdvertisedIP: "203.0.113.10",
		ShredVersion: 4321,
		Identity:     holder,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.initializeContact(&net.UDPAddr{IP: net.IPv4zero, Port: 65400}); err != nil {
		t.Fatalf("initializeContact: %v", err)
	}
	defer holder.OnSwap(client.adoptIdentity)()

	// The new identity's previous owner published these before the swap.
	newPubkey := Pubkey(newKey.Public().(ed25519.PublicKey))
	previousOwner, err := NewContactInfo(newPubkey, 4321,
		&net.UDPAddr{IP: net.ParseIP("198.51.100.7"), Port: 9000},
		&net.UDPAddr{IP: net.ParseIP("198.51.100.7"), Port: 9001})
	if err != nil {
		t.Fatalf("NewContactInfo: %v", err)
	}
	beforeSwap := wallclockMillis() - 1000

	if _, err := holder.Swap(newKey); err != nil {
		t.Fatalf("Swap: %v", err)
	}
	if client.Pubkey() != newPubkey {
		t.Fatalf("client pubkey = %s, want %s", client.Pubkey(), newPubkey)
	}
	select {
	case <-client.contactChanged:
	default:
		t.Fatal("swap did not schedule an immediate contact push")
	}
	value, kp, err := client.currentContactValue()
	if err != nil {
		t.Fatalf("currentContactValue: %v", err)
	}
	if Pubkey(kp.PublicKey()) != newPubkey || value.ContactInfo.Pubkey != newPubkey || !value.Verify() {
		t.Fatalf("contact is not signed by the new identity: pubkey=%s signer=%s verified=%v", value.ContactInfo.Pubkey, kp.PublicKey(), value.Verify())
	}

	for i := range uint64(2) {
		client.handleContactRecord(contactRecordFromInfo(t, previousOwner.CloneWithWallclock(beforeSwap+i), newKey), 4321)
	}
	select {
	case err := <-client.identityConflictCh:
		t.Fatalf("records from before the swap signaled a conflict: %v", err)
	default:
	}

	// A live second owner of the new identity is still detected.
	future := wallclockMillis() + 60_000
	for i := range uint64(2) {
		client.handleContactRecord(contactRecordFromInfo(t, previousOwner.CloneWithWallclock(future+i), newKey), 4321)
	}
	select {
	case err := <-client.identityConflictCh:
		if !errors.Is(err, ErrIdentityInUse) {
			t.Fatalf("conflict error = %v", err)
		}
	default:
		t.Fatal("live conflicting owner of the new identity was not detected")
	}
}
