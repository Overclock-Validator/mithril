package adminrpc

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Client is a connection to a node's admin RPC socket.
type Client struct {
	conn    net.Conn
	decoder *json.Decoder
	nextID  int64
}

// Dial connects to the admin socket of the node whose ledger is ledger.
func Dial(ledger string) (*Client, error) {
	path := SocketPath(ledger)
	conn, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect to admin RPC at %s: %w", path, err)
	}
	return &Client{conn: conn, decoder: json.NewDecoder(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// SetIdentityFromBytes asks the node to switch to the 64-byte Solana keypair
// key. The node's error is returned as an *Error.
func (c *Client) SetIdentityFromBytes(key []byte, requireTower, requireVoteHistory bool) error {
	// encoding/json writes []byte as base64; Agave sends a number array.
	values := make([]int, len(key))
	for i, b := range key {
		values[i] = int(b)
	}
	return c.call("setIdentityFromBytes", []any{values, requireTower, requireVoteHistory})
}

// SetIdentity asks the node to read its new identity from keypairFile, which
// the node process must be able to read.
func (c *Client) SetIdentity(keypairFile string, requireTower, requireVoteHistory bool) error {
	return c.call("setIdentity", []any{keypairFile, requireTower, requireVoteHistory})
}

func (c *Client) call(method string, params []any) error {
	c.nextID++
	id, err := json.Marshal(c.nextID)
	if err != nil {
		return err
	}
	encodedParams, err := json.Marshal(params)
	if err != nil {
		return err
	}
	data, err := json.Marshal(request{JSONRPC: "2.0", ID: id, Method: method, Params: encodedParams})
	if err != nil {
		return err
	}
	if _, err := c.conn.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("send %s: %w", method, err)
	}
	var resp response
	if err := c.decoder.Decode(&resp); err != nil {
		return fmt.Errorf("read %s response: %w", method, err)
	}
	if resp.Error != nil {
		return resp.Error
	}
	return nil
}
