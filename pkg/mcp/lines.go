package mcp

import (
	"bufio"
	"io"
)

// readCappedLine reads one line without its newline and consumes any bytes
// beyond max so the next read starts at the next line.
func readCappedLine(r *bufio.Reader, max int) (line []byte, oversize, terminated, eof bool, err error) {
	var out []byte
	for {
		frag, readErr := r.ReadSlice('\n')
		if len(frag) > 0 {
			end := len(frag)
			hasNewline := readErr == nil
			if hasNewline {
				end--
			}
			if !oversize {
				room := max - len(out)
				if end <= room {
					out = append(out, frag[:end]...)
				} else {
					if room > 0 {
						out = append(out, frag[:room]...)
					}
					oversize = true
				}
			}
			if hasNewline {
				return out, oversize, true, false, nil
			}
		}
		switch readErr {
		case bufio.ErrBufferFull:
			continue
		case io.EOF:
			if len(out) > 0 || oversize {
				return out, oversize, false, false, nil
			}
			return nil, false, false, true, nil
		case nil:
		default:
			return nil, false, false, false, readErr
		}
	}
}
