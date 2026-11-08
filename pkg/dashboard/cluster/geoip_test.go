package cluster

import (
	"bytes"
	"compress/gzip"
	"net/netip"
	"testing"
)

func TestPackagedCountryDatabase(t *testing.T) {
	db, err := countryReader()
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"8.8.8.8", "2001:4860:4860::8888"} {
		country, err := countryForIP(db, netip.MustParseAddr(address))
		if err != nil || !countryOK(country) {
			t.Errorf("lookup %s: country %q, error %v", address, country, err)
		}
	}
}

func TestInvalidCountryDatabase(t *testing.T) {
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	if _, err := z.Write([]byte("not a database")); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{nil, []byte("not gzip"), compressed.Bytes()} {
		if db, err := openCountryDatabase(data); err == nil {
			_ = db.Close()
			t.Fatal("invalid database accepted")
		}
	}
}
