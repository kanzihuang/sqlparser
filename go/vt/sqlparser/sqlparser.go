package sqlparser

import (
	"io"
	"strings"
)

func (tkn *Tokenizer) cur() uint16 {
	return tkn.buf.Cur()
}

func (tkn *Tokenizer) skip(dist int) {
	tkn.buf.Skip(dist)
}

func (tkn *Tokenizer) peek(dist int) uint16 {
	return tkn.buf.Peek(dist)
}

func (tkn *Tokenizer) next() {
	tkn.buf.Next()
}

func (tkn *Tokenizer) readBuffer() string {
	return tkn.buf.ReadBuffer()
}

func (tkn *Tokenizer) readCache() string {
	return tkn.buf.ReadCache()
}

func (tkn *Tokenizer) resetCache() {
	tkn.buf.ResetCache()
}

func (tkn *Tokenizer) absolutePos() int {
	return tkn.buf.AbsolutePos()
}

func (tkn *Tokenizer) absoluteStart() int {
	return tkn.buf.AbsoluteStart()
}

func scanProcedureBody(tokenizer *Tokenizer, tagName string) (string, error) {
	var sb strings.Builder
	tokenizer.buf.CacheBlanks = true
	defer func() {
		tokenizer.buf.CacheBlanks = false
	}()
loop:
	for {
		tkn, val := tokenizer.Scan()
		switch tkn {
		case 0, eofChar:
			break loop
		case ID:
			sb.WriteString(tokenizer.readCache())
			if val == tagName {
				break loop
			}
		default:
			sb.WriteString(tokenizer.readCache())
		}
	}
	if tokenizer.LastError != nil {
		return "", tokenizer.LastError
	}
	if sb.Len() == 0 {
		return "", io.EOF
	}
	return sb.String(), nil
}

// SplitNext returns the next sql statement or EOF.
// WithCacheInBuffer()(tokenizer) must be called before SplitNext.
func SplitNext(tokenizer *Tokenizer) (string, error) {
	var sb strings.Builder
loop:
	for {
		tkn, val := tokenizer.Scan()
		switch tkn {
		case COMMENT:
			tokenizer.resetCache()
		case ';':
			tokenizer.resetCache()
			if sb.Len() > 0 {
				break loop
			}
		case 0, eofChar:
			break loop
		case ID:
			if len(val) > 1 && val[0] == '$' && val[len(val)-1] == '$' {
				body, err := scanProcedureBody(tokenizer, val)
				if err != nil {
					break loop
				}
				sb.WriteString(body)
				break
			}
			sb.WriteString(tokenizer.readCache())
		default:
			sb.WriteString(tokenizer.readCache())
		}
	}
	if tokenizer.LastError != nil {
		return "", tokenizer.LastError
	}
	if sb.Len() == 0 {
		return "", io.EOF
	}
	return strings.TrimSpace(sb.String()), nil
}
