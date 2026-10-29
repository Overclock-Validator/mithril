package rpcserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/DataDog/zstd"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
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
	GetAccountEncodingBinary
)

func (rpcServer *RpcServer) GetAccountInfo(ctx context.Context, p jsonrpc.RawParams) (GetAccountInfoResp, error) {
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil {
		return GetAccountInfoResp{}, &InvalidParamsError{Message: "invalid getAccountInfo parameters"}
	}

	if len(params) < 1 || len(params) > 2 {
		return GetAccountInfoResp{}, &InvalidParamsError{Message: "getAccountInfo requires a key and optional config"}
	}
	var pkStr string
	if err := json.Unmarshal(params[0], &pkStr); err != nil || len(pkStr) > 44 {
		return GetAccountInfoResp{}, &InvalidParamsError{Message: "invalid account key"}
	}

	pk, err := solana.PublicKeyFromBase58(pkStr)
	if err != nil {
		return GetAccountInfoResp{}, &InvalidParamsError{Message: "invalid base58 encoding"}
	}

	conf, err := parseAccountConfig(params, nil)
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

	val, err := accountInfoValue(acct, conf)
	if err != nil {
		return GetAccountInfoResp{}, err
	}
	return GetAccountInfoResp{Context: GetAccountInfoRespContext{ApiVersion: "mithril 0.1", Slot: rooted.Slot}, Value: val}, nil
}

func accountInfoValue(acct *accounts.Account, conf *GetAccountInfoConfig) (*GetAccountInfoRespValue, error) {
	if acct == nil || acct.Lamports == 0 {
		return nil, nil
	}
	var acctData any
	var err error
	if conf.EncodingType != nil && *conf.EncodingType == GetAccountEncodingJson && acct.Owner == addresses.VoteProgramAddr {
		acctData, err = parsedVoteAccountData(acct.Data, acct.Key)
	}
	if acctData == nil || err != nil {
		acctData, err = encodeAcctDataWithConfig(acct.Data, conf)
	}
	if err != nil {
		return nil, err
	}

	val := &GetAccountInfoRespValue{
		Data:       acctData,
		Executable: acct.Executable,
		Lamports:   acct.Lamports,
		Owner:      base58.Encode(acct.Owner[:]),
		RentEpoch:  acct.RentEpoch,
		Space:      uint64(len(acct.Data))}

	return val, nil
}

func parseAccountConfig(params []json.RawMessage, defaultEncoding *int) (*GetAccountInfoConfig, error) {
	var input struct {
		Commitment     *string `json:"commitment"`
		Encoding       *string `json:"encoding"`
		MinContextSlot *uint64 `json:"minContextSlot"`
		DataSlice      *struct {
			Length *uint64 `json:"length"`
			Offset *uint64 `json:"offset"`
		} `json:"dataSlice"`
	}
	if len(params) > 1 {
		if err := json.Unmarshal(params[1], &input); err != nil {
			return nil, &InvalidParamsError{Message: "invalid account config"}
		}
	}
	conf := &GetAccountInfoConfig{EncodingType: defaultEncoding}
	if input.Commitment != nil {
		if err := validateRootedCommitment(*input.Commitment); err != nil {
			return nil, err
		}
		conf.Commitment = *input.Commitment
	}
	if input.MinContextSlot != nil {
		conf.MinContextSlot = *input.MinContextSlot
	}
	if input.Encoding != nil {
		encoding, err := parseGetAcctDataEncodingType(*input.Encoding)
		if err != nil {
			return nil, &InvalidParamsError{Message: err.Error()}
		}
		conf.EncodingType = &encoding
	}
	if input.DataSlice != nil {
		if input.DataSlice.Length == nil || input.DataSlice.Offset == nil {
			return nil, &InvalidParamsError{Message: "dataSlice requires length and offset"}
		}
		conf.DataSlice = &GetAccountInfoDataSlice{Len: input.DataSlice.Length, Offset: input.DataSlice.Offset}
		if conf.EncodingType != nil && *conf.EncodingType == GetAccountEncodingJson {
			return nil, &InvalidParamsError{Message: "cannot use jsonParsed with dataSlice"}
		}
	}
	return conf, nil
}

func parseGetAcctDataEncodingType(encodingStr string) (int, error) {
	switch encodingStr {
	case "binary":
		return GetAccountEncodingBinary, nil
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
	selected := extractRequestedAcctData(data, length, offset)
	if (config.EncodingType == nil || *config.EncodingType == GetAccountEncodingBase58 || *config.EncodingType == GetAccountEncodingBinary) && len(selected) > 128 {
		return nil, &accountEncodingError{}
	}

	var encodingTypeStr string
	var dataStr string
	var dataObj interface{}

	if config.EncodingType != nil {
		switch *config.EncodingType {
		case GetAccountEncodingBinary:
			return base58.Encode(selected), nil
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
				compressedBytes, err := zstd.Compress(nil, acctData)
				if err != nil {
					encodingTypeStr = "base64"
					compressedBytes = acctData
				}
				dataStr = base64.StdEncoding.EncodeToString(compressedBytes)
			}
		}

		dataObj = []string{dataStr, encodingTypeStr}
	} else {
		dataObj = base58.Encode(selected)
	}

	return dataObj, nil
}

type accountEncodingError struct{}

func (*accountEncodingError) Error() string {
	return "Encoded binary (base 58) data should be less than 128 bytes, please use Base64 encoding."
}
func (e *accountEncodingError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{Code: -32600, Message: e.Error()}, nil
}
func (*accountEncodingError) FromJSONRPCError(value jsonrpc.JSONRPCError) error {
	if value.Code != -32600 {
		return fmt.Errorf("unexpected account encoding error code %d", value.Code)
	}
	return nil
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
