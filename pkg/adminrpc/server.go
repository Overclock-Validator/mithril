// Package adminrpc is Mithril's port of Agave's admin RPC: newline-delimited
// JSON-RPC 2.0 on a Unix socket at <ledger>/admin.rpc, reachable only by the
// node's own user. It is separate from the public Solana JSON-RPC server and
// implements setIdentity and setIdentityFromBytes.
package adminrpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// SocketName is the admin socket's file name inside the ledger directory,
// matching Agave.
const SocketName = "admin.rpc"

// SocketPath returns the admin socket path for ledger.
func SocketPath(ledger string) string {
	return filepath.Join(ledger, SocketName)
}

// JSON-RPC 2.0 error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// Error is a JSON-RPC error object. Handlers return it to choose the code the
// client sees; any other error is reported as an internal error.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	return e.Message
}

func invalidParams(format string, args ...any) *Error {
	return &Error{Code: CodeInvalidParams, Message: fmt.Sprintf(format, args...)}
}

// IdentitySetter performs a set-identity request.
type IdentitySetter interface {
	SetIdentity(key []byte, requireTower, requireVoteHistory bool) error
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type response struct {
	JSONRPC string           `json:"jsonrpc"`
	Result  *json.RawMessage `json:"result,omitempty"`
	Error   *Error           `json:"error,omitempty"`
	ID      json.RawMessage  `json:"id"`
}

var nullResult = json.RawMessage("null")

// Server serves the admin RPC socket until Close.
type Server struct {
	path     string
	listener net.Listener
	setter   IdentitySetter
	wg       sync.WaitGroup

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// Listen creates the admin socket at path with mode 0600 and starts serving.
// A stale socket left by a previous process is replaced; a live one is not.
func Listen(path string, setter IdentitySetter) (*Server, error) {
	if setter == nil {
		return nil, errors.New("admin RPC requires an identity setter")
	}
	if err := removeStaleSocket(path); err != nil {
		return nil, err
	}
	// Bind under a temporary name and rename once the mode is 0600, so the
	// socket is never reachable at path with looser permissions.
	tmp := fmt.Sprintf("%s.%d.tmp", path, os.Getpid())
	_ = os.Remove(tmp)
	listener, err := net.Listen("unix", tmp)
	if err != nil {
		return nil, fmt.Errorf("listen on admin RPC socket %s: %w", path, err)
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := os.Chmod(tmp, 0o600); err != nil {
		listener.Close()
		os.Remove(tmp)
		return nil, fmt.Errorf("restrict admin RPC socket %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		listener.Close()
		os.Remove(tmp)
		return nil, fmt.Errorf("publish admin RPC socket %s: %w", path, err)
	}
	s := &Server{path: path, listener: listener, setter: setter, conns: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect admin RPC socket %s: %w", path, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("admin RPC socket path %s exists and is not a socket", path)
	}
	if conn, err := net.DialTimeout("unix", path, time.Second); err == nil {
		conn.Close()
		return fmt.Errorf("admin RPC socket %s is in use by another process", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale admin RPC socket %s: %w", path, err)
	}
	return nil
}

// Path is the socket's path.
func (s *Server) Path() string {
	return s.path
}

// Close stops accepting, closes open connections, waits for in-flight
// requests, and removes the socket file.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	err := s.listener.Close()
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	if rerr := os.Remove(s.path); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
		err = errors.Join(err, rerr)
	}
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				mlog.Log.Warnf("admin RPC accept on %s: %v", s.path, err)
			}
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		conn.Close()
	}()
	decoder := json.NewDecoder(conn)
	writer := bufio.NewWriter(conn)
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				// The stream cannot be resynchronized after malformed JSON.
				_ = writeResponse(writer, response{JSONRPC: "2.0", Error: &Error{Code: CodeParseError, Message: "Parse error"}, ID: nullResult})
			}
			return
		}
		resp, reply := s.handle(raw)
		if !reply {
			continue
		}
		if err := writeResponse(writer, resp); err != nil {
			return
		}
	}
}

func writeResponse(w *bufio.Writer, resp response) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	if _, err := w.Write(append(data, '\n')); err != nil {
		return err
	}
	return w.Flush()
}

// handle runs one request. Notifications, which carry no id, get no reply.
func (s *Server) handle(raw json.RawMessage) (response, bool) {
	var req request
	if err := json.Unmarshal(raw, &req); err != nil || req.JSONRPC != "2.0" || req.Method == "" {
		return response{JSONRPC: "2.0", Error: &Error{Code: CodeInvalidRequest, Message: "Invalid request"}, ID: nullResult}, true
	}
	err := s.dispatch(req.Method, req.Params)
	if len(req.ID) == 0 {
		return response{}, false
	}
	resp := response{JSONRPC: "2.0", ID: req.ID}
	if err != nil {
		var rpcErr *Error
		if !errors.As(err, &rpcErr) {
			rpcErr = &Error{Code: CodeInternalError, Message: err.Error()}
		}
		resp.Error = rpcErr
	} else {
		resp.Result = &nullResult
	}
	return resp, true
}

func (s *Server) dispatch(method string, params json.RawMessage) error {
	switch method {
	case "setIdentity":
		var path string
		requireTower, requireVoteHistory, err := decodeSetIdentityParams(params, &path)
		if err != nil {
			return err
		}
		key, err := ReadKeypairFile(path)
		if err != nil {
			return invalidParams("Failed to read identity keypair from %s: %v", path, err)
		}
		return s.setter.SetIdentity(key, requireTower, requireVoteHistory)
	case "setIdentityFromBytes":
		var values []int64
		requireTower, requireVoteHistory, err := decodeSetIdentityParams(params, &values)
		if err != nil {
			return err
		}
		key, err := keypairBytes(values)
		if err != nil {
			return invalidParams("Failed to read identity keypair from provided byte array: %v", err)
		}
		return s.setter.SetIdentity(key, requireTower, requireVoteHistory)
	default:
		return &Error{Code: CodeMethodNotFound, Message: "Method not found"}
	}
}

// decodeSetIdentityParams decodes Agave's positional parameters
// [identity, require_tower, require_vote_history] into identity and the flags.
func decodeSetIdentityParams(params json.RawMessage, identity any) (requireTower, requireVoteHistory bool, err error) {
	var positional []json.RawMessage
	if err := json.Unmarshal(params, &positional); err != nil || len(positional) != 3 {
		return false, false, invalidParams("Invalid params: expected [identity, require_tower, require_vote_history]")
	}
	if err := json.Unmarshal(positional[0], identity); err != nil {
		return false, false, invalidParams("Invalid params: identity: %v", err)
	}
	if err := json.Unmarshal(positional[1], &requireTower); err != nil {
		return false, false, invalidParams("Invalid params: require_tower must be a boolean")
	}
	if err := json.Unmarshal(positional[2], &requireVoteHistory); err != nil {
		return false, false, invalidParams("Invalid params: require_vote_history must be a boolean")
	}
	return requireTower, requireVoteHistory, nil
}

// keypairBytes converts a JSON number array, as Solana keypair files and
// Agave's setIdentityFromBytes carry them, into bytes.
func keypairBytes(values []int64) ([]byte, error) {
	key := make([]byte, len(values))
	for i, v := range values {
		if v < 0 || v > 255 {
			return nil, fmt.Errorf("byte %d is out of range: %d", i, v)
		}
		key[i] = byte(v)
	}
	return key, nil
}

// ReadKeypairFile reads a Solana keygen JSON keypair file without checking
// its length; the identity setter validates the key.
func ReadKeypairFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var values []int64
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, fmt.Errorf("not a JSON keypair: %w", err)
	}
	return keypairBytes(values)
}
