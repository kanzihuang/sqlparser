package sqlparser

import "io"

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

// SplitNext returns the next sql statement or EOF.
// WithBufferCache()(tokenizer) must be called before SplitNext.
func SplitNext(tokenizer *Tokenizer) (string, error) {
	var statement string
	tkn := 0
	emptyStatement := true
loop:
	for {
		tkn, _ = tokenizer.Scan()
		switch tkn {
		case ';':
			if !emptyStatement {
				statement = tokenizer.readCache()
				statement = statement[:len(statement)-1]
				break loop
			}
			tokenizer.resetCache()
		case 0, eofChar:
			if !emptyStatement {
				statement = tokenizer.readCache()
			}
			break loop
		default:
			emptyStatement = false
		}
	}
	if tokenizer.LastError != nil {
		return "", tokenizer.LastError
	}
	if len(statement) == 0 {
		return "", io.EOF
	}
	return statement, nil
}
