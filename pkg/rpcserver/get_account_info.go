package rpcserver

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"

	"github.com/DataDog/zstd"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
)

type GetAccountInfoConfig struct {
	Commitment     string
	EncodingType   *int
	DataSlice      *GetAccountInfoDataSlice
	MinContextSlot uint64
}

type GetAccountInfoDataSlice struct {
	Len    *uint64
	Offset *uint64
}

type GetAccountInfoRespContext struct {
	ApiVersion string `json:"apiVersion"`
	Slot       uint64 `json:"slot"`
}

type GetAccountInfoRespValue struct {
	Data       any    `json:"data"`
	Executable bool   `json:"executable"`
	Lamports   uint64 `json:"lamports"`
	Owner      string `json:"owner"`
	RentEpoch  uint64 `json:"rentEpoch"`
	Space      uint64 `json:"space"`
}

type GetAccountInfoResp struct {
	Context GetAccountInfoRespContext `json:"context"`
	Value   *GetAccountInfoRespValue  `json:"value"`
}

const (
	GetAccountEncodingBase58 = iota
	GetAccountEncodingBase64
	GetAccountEncodingBase64Zstd
	GetAccountEncodingJson
)

func (rpcServer *RpcServer) GetAccountInfo(ctx context.Context, p jsonrpc.RawParams) (GetAccountInfoResp, error) {
	params, err := jsonrpc.DecodeParams[[]interface{}](p)
	if err != nil {
		return GetAccountInfoResp{}, fmt.Errorf("decoding params: %w", err)
	}

	if len(params) < 1 {
		return GetAccountInfoResp{}, fmt.Errorf("getAccountInfo requires a string as first parameter")
	}

	pkStr, ok := params[0].(string)
	if !ok {
		return GetAccountInfoResp{}, fmt.Errorf("getAccountInfo requires a string as first parameter")
	}

	pk, err := solana.PublicKeyFromBase58(pkStr)
	if err != nil {
		return GetAccountInfoResp{}, fmt.Errorf("invalid base58 encoding")
	}

	conf, err := parseGetAccountInfoConfMap(params)
	if err != nil {
		return GetAccountInfoResp{}, err
	}
	rooted, acct, err := rpcServer.readRootedAccount(ctx, pk)
	if err != nil && !errors.Is(err, accountsdb.ErrNoAccount) {
		return GetAccountInfoResp{}, fmt.Errorf("read account at rooted slot %d: %w", rooted.Slot, err)
	}
	if conf.MinContextSlot > rooted.Slot {
		return GetAccountInfoResp{}, &MinContextSlotNotReachedError{ContextSlot: rooted.Slot}
	}
	if errors.Is(err, accountsdb.ErrNoAccount) {
		return GetAccountInfoResp{Context: GetAccountInfoRespContext{ApiVersion: "mithril 0.1", Slot: rooted.Slot}}, nil
	}

	var acctData interface{}
	if conf.EncodingType != nil && *conf.EncodingType == GetAccountEncodingJson && conf.DataSlice != nil {
		return GetAccountInfoResp{}, fmt.Errorf("cannot use jsonParsed with dataSlice")
	}
	if conf.EncodingType != nil && *conf.EncodingType == GetAccountEncodingJson && acct.Owner == addresses.VoteProgramAddr {
		acctData, err = parsedVoteAccountData(acct.Data, pk)
	}
	if acctData == nil || err != nil {
		acctData, err = encodeAcctDataWithConfig(acct.Data, conf)
	}
	if err != nil {
		return GetAccountInfoResp{}, err
	}

	val := &GetAccountInfoRespValue{
		Data:       acctData,
		Executable: acct.Executable,
		Lamports:   acct.Lamports,
		Owner:      base58.Encode(acct.Owner[:]),
		RentEpoch:  acct.RentEpoch,
		Space:      uint64(len(acct.Data))}

	return GetAccountInfoResp{Context: GetAccountInfoRespContext{ApiVersion: "mithril 0.1", Slot: rooted.Slot}, Value: val}, nil
}

func parseGetAccountInfoConfMap(params []interface{}) (*GetAccountInfoConfig, error) {
	if len(params) < 2 {
		return &GetAccountInfoConfig{}, nil
	}

	var err error
	confMap, ok := params[1].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid config object")
	}

	conf := &GetAccountInfoConfig{}

	// commitment is optional
	commitmentObj, ok := confMap["commitment"]
	if ok {
		commitmentStr, ok := commitmentObj.(string)
		if !ok || (commitmentStr != "processed" && commitmentStr != "confirmed" && commitmentStr != "finalized") {
			return nil, fmt.Errorf("invalid commitment")
		}
		conf.Commitment = commitmentStr
	}

	if minContextSlot, ok := confMap["minContextSlot"]; ok {
		conf.MinContextSlot, err = rpcUint64(minContextSlot, "minContextSlot")
		if err != nil {
			return nil, err
		}
	}

	// encoding type is optional. defaults to base58.
	encodingObj, ok := confMap["encoding"]
	if ok {
		encodingStr, ok := encodingObj.(string)
		if ok {
			var encodingType int
			encodingType, err = parseGetAcctDataEncodingType(encodingStr)
			if err != nil {
				return nil, err
			}
			conf.EncodingType = &encodingType
		} else {
			return nil, fmt.Errorf("invalid encoding type")
		}
	}

	// dataSlice is optional. if present, it must contain both 'length' and 'offset' fields.
	dataSliceObj, ok := confMap["dataSlice"]
	if ok {
		mlog.Log.Infof("dataSliceObj: %+v, type: %s", dataSliceObj, reflect.TypeOf(dataSliceObj))
		dsMap, ok := dataSliceObj.(map[string]interface{})
		if ok {
			conf.DataSlice, err = parseDataSlice(dsMap)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("invalid dataSlice")
		}
	}

	return conf, nil
}

func parseDataSlice(dataSlice map[string]interface{}) (*GetAccountInfoDataSlice, error) {
	lengthObj, hasLen := dataSlice["length"]
	offsetObj, hasOffset := dataSlice["offset"]

	if hasLen && !hasOffset {
		return nil, fmt.Errorf("missing dataSlice field offset")
	} else if !hasLen && hasOffset {
		return nil, fmt.Errorf("missing dataSlice field length")
	}

	ds := &GetAccountInfoDataSlice{}
	if hasLen == hasOffset && hasLen {
		length, ok := lengthObj.(float64)
		if !ok {
			return nil, fmt.Errorf("invalid dataSlice length, expected usize, got %s", reflect.TypeOf(lengthObj))
		}
		offset, ok := offsetObj.(float64)
		if !ok {
			return nil, fmt.Errorf("invalid dataSlice offset, expected usize, got %s", reflect.TypeOf(offsetObj))
		}

		lengthUint := uint64(length)
		offsetUint := uint64(offset)
		ds.Len = &lengthUint
		ds.Offset = &offsetUint
	}

	return ds, nil
}

func parseGetAcctDataEncodingType(encodingStr string) (int, error) {
	switch encodingStr {
	case "base58":
		return GetAccountEncodingBase58, nil

	case "base64":
		return GetAccountEncodingBase64, nil

	case "base64+zstd":
		return GetAccountEncodingBase64Zstd, nil

	case "jsonParsed":
		return GetAccountEncodingJson, nil

	default:
		return 0, fmt.Errorf("invalid data encoding %s", encodingStr)
	}
}

func encodeAcctDataWithConfig(data []byte, config *GetAccountInfoConfig) (interface{}, error) {
	var offset uint64
	var length uint64

	length = uint64(len(data))
	if config.DataSlice != nil {
		if config.DataSlice.Len != nil {
			length = *config.DataSlice.Len
		}

		if config.DataSlice.Offset != nil {
			offset = *config.DataSlice.Offset
		}
	}

	var encodingTypeStr string
	var dataStr string
	var dataObj interface{}

	if config.EncodingType != nil {
		switch *config.EncodingType {
		case GetAccountEncodingBase58:
			{
				encodingTypeStr = "base58"
				acctData := extractRequestedAcctData(data, length, offset)
				if len(acctData) != 0 {
					dataStr = base58.Encode(acctData)
				}
			}

		case GetAccountEncodingJson:
			{
				if config.DataSlice != nil {
					return nil, fmt.Errorf("cannot use jsonParsed with dataSlice")
				}
			}

			fallthrough
		case GetAccountEncodingBase64:
			{
				encodingTypeStr = "base64"
				acctData := extractRequestedAcctData(data, length, offset)
				if len(acctData) != 0 {
					dataStr = base64.StdEncoding.EncodeToString(acctData)
				}
			}

		case GetAccountEncodingBase64Zstd:
			{
				encodingTypeStr = "base64+zstd"
				acctData := extractRequestedAcctData(data, length, offset)
				compressedBytes, _ := zstd.Compress(nil, acctData)
				dataStr = base64.StdEncoding.EncodeToString(compressedBytes)
			}
		}

		dataObj = []string{dataStr, encodingTypeStr}
	} else {
		acctData := extractRequestedAcctData(data, length, offset)
		if len(acctData) != 0 {
			dataObj = base58.Encode(acctData)
		}
	}

	return dataObj, nil
}

func extractRequestedAcctData(data []byte, length uint64, offset uint64) []byte {
	if offset > uint64(len(data)) {
		return nil
	}

	end := safemath.SaturatingAddU64(length, offset)
	if end > uint64(len(data)) {
		return data[offset:]
	}

	return data[offset : offset+length]
}
