package rpcserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/filecoin-project/go-jsonrpc"
)

func decodeOptionalParams(p jsonrpc.RawParams) ([]interface{}, error) {
	if len(p) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(p))
	decoder.UseNumber()
	var params []interface{}
	if err := decoder.Decode(&params); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return nil, fmt.Errorf("parameters must contain one JSON value")
	}
	return params, nil
}

// validateRootedCommitment rejects requests that need an unpublished account view.
func validateRootedCommitment(raw interface{}) error {
	if raw == nil {
		return nil
	}
	value, ok := raw.(string)
	if !ok {
		return &InvalidParamsError{Message: "invalid commitment"}
	}
	switch value {
	case "confirmed", "finalized":
		return nil
	case "processed":
		return &InvalidParamsError{Message: "processed commitment is not supported by this method; use confirmed or finalized"}
	default:
		return &InvalidParamsError{Message: "invalid commitment"}
	}
}
