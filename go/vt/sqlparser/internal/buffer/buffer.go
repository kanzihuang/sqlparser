package buffer

import (
	"bytes"
	"io"
)

const (
	eofChar           = 0x100
	defaultBufferSize = 0x1000
)

type Buffer struct {
	reader      io.Reader
	offset      int
	buf         []byte
	start       int
	pos         int
	eof         bool
	cache       *bytes.Buffer
	cacheOffset int
}

func NewStringBuffer(sql string) *Buffer {
	buf := &Buffer{
		// for testing
		//reader: strings.NewReader(sql),
		buf: []byte(sql),
	}
	return buf
}

type BufferOpt func(*Buffer)

func WithCache() BufferOpt {
	return func(buffer *Buffer) {
		if buffer.cache == nil {
			buffer.cache = &bytes.Buffer{}
		}
	}
}

func WithoutCache() BufferOpt {
	return func(buffer *Buffer) {
		if buffer.cache != nil {
			buffer.cache = nil
		}
	}
}

func NewReaderBuffer(reader io.Reader, opts ...BufferOpt) *Buffer {
	buf := &Buffer{
		reader: reader,
	}
	for _, opt := range opts {
		opt(buf)
	}
	return buf
}

func (tb *Buffer) AbsolutePos() int {
	return tb.offset + tb.pos
}

func (tb *Buffer) AbsoluteStart() int {
	return tb.offset + tb.pos
}

func (tb *Buffer) Cur() uint16 {
	return tb.Peek(0)
}

func (tb *Buffer) Peek(dist int) uint16 {
	if tb.pos+dist >= len(tb.buf) {
		if err := tb.load(); err == io.EOF {
			return eofChar
		} else if err != nil {
			panic("sqlparser: " + err.Error())
		}
		if tb.pos+dist >= len(tb.buf) {
			panic("sqlparser: invalid buffer position")
		}
	}
	return uint16(tb.buf[tb.pos+dist])
}

func (tb *Buffer) Skip(dist int) {
	tb.pos += dist
	if tb.cache != nil {
		tb.cache.Write(tb.buf[tb.start:tb.pos])
	}
	tb.start = tb.pos
}

func (tb *Buffer) HalfFull() bool {
	return tb.pos-tb.start > defaultBufferSize/2
}

func (tb *Buffer) ReadBuffer() string {
	pos := tb.pos
	if pos > len(tb.buf) {
		pos = len(tb.buf)
	}
	result := tb.buf[tb.start:pos]
	tb.start = tb.pos
	if tb.cache != nil {
		tb.cache.Write(result)
	}
	return string(result)
}

func (tb *Buffer) ReadCache() string {
	if tb.cache == nil && tb.reader == nil {
		result := string(tb.buf[tb.cacheOffset:tb.AbsolutePos()])
		tb.cacheOffset = tb.AbsolutePos()
		return result
	}

	if tb.cache == nil {
		panic("read from null cache of in tokenizer buffer")
	}

	result := tb.cache.String()
	tb.cache.Reset()
	return result
}

func (tb *Buffer) ResetCache() {
	if tb.cache == nil && tb.reader == nil {
		tb.cacheOffset = tb.AbsolutePos()
		return
	}
	if tb.cache == nil {
		panic("reset from null cache of in tokenizer buffer")
	}
	tb.cache.Reset()
}

func (tb *Buffer) Next() {
	tb.pos++
}

func (tb *Buffer) load() error {
	if tb.reader == nil || tb.eof {
		return io.EOF
	}

	buf := tb.buf
	size := len(buf)
	if size < defaultBufferSize {
		size = defaultBufferSize
	}
	if size > len(buf) {
		buf = make([]byte, size)
	}
	copy(buf, tb.buf[tb.start:])

	tb.offset += tb.start
	tb.pos -= tb.start
	size = len(tb.buf) - tb.start
	tb.start = 0
	tb.buf = buf

	for size < len(tb.buf) {
		n, err := tb.reader.Read(tb.buf[size:])
		size += n
		if err == io.EOF {
			tb.eof = true
			tb.buf = tb.buf[:size]
			if tb.pos >= size {
				return err
			}
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}
