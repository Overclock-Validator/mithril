package cluster

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"errors"
	"io"
	"net/netip"
	"sync"

	"github.com/oschwald/maxminddb-golang/v2"
)

//go:embed dbip-country.mmdb.gz
var countryDatabase []byte

var (
	countryOnce sync.Once
	countryDB   *maxminddb.Reader
	countryErr  error
)

func countryReader() (*maxminddb.Reader, error) {
	countryOnce.Do(func() { countryDB, countryErr = openCountryDatabase(countryDatabase) })
	return countryDB, countryErr
}

func openCountryDatabase(compressed []byte) (*maxminddb.Reader, error) {
	z, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer z.Close()
	const limit = 32 << 20
	data, err := io.ReadAll(io.LimitReader(z, limit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, errors.New("country database exceeds limit")
	}
	db, err := maxminddb.OpenBytes(data)
	if err != nil {
		return nil, err
	}
	if err := db.Verify(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func countryForIP(db *maxminddb.Reader, ip netip.Addr) (string, error) {
	var result struct {
		Country struct {
			Code string `maxminddb:"iso_code"`
		} `maxminddb:"country"`
	}
	if err := db.Lookup(ip).Decode(&result); err != nil {
		return "", err
	}
	if !countryOK(result.Country.Code) {
		return "", nil
	}
	return result.Country.Code, nil
}
