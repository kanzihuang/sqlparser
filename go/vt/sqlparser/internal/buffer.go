package internal

import (
	"io"
)

const (
	eofChar           = 0x100
	defaultBufferSize = 0x1000
)

type Buffer struct {
	reader io.Reader
	offset int
	buf    []byte
	start  int
	pos    int
	eof    bool
}

func NewStringBuffer(sql string) *Buffer {
	buf := &Buffer{
		// for testing
		//reader: strings.NewReader(sql),
		buf: []byte(sql),
	}
	return buf
}

func NewReaderBuffer(reader io.Reader) *Buffer {
	buf := &Buffer{
		reader: reader,
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
	tb.start = tb.pos
}

func (tb *Buffer) HalfFull() bool {
	return tb.pos-tb.start > defaultBufferSize/2
}

func (tb *Buffer) Read() string {
	pos := tb.pos
	if pos > len(tb.buf) {
		pos = len(tb.buf)
	}
	result := string(tb.buf[tb.start:pos])
	tb.start = tb.pos
	return result
}

func (tb *Buffer) Back(dist int) {
	tb.start -= dist
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
	//if tb.pos-tb.start > size/2 {
	//	if len(buf) >= maxBufferSize {
	//		return errors.New(fmt.Sprintf("sqlparser: too long token(greater than %d): %s...%s",
	//			tb.pos-tb.start, tb.buf[:tb.start+32], tb.buf[tb.pos-32:]))
	//	}
	//	size <<= 1
	//	if size > maxBufferSize {
	//		size = maxBufferSize
	//	}
	//}
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

func (tb *Buffer) reset() {
	if tb.reader == nil {
		return
	}
	size := len(tb.buf)
	if tb.pos < 0 || tb.pos > size {
		panic("sqlparser: invalid buffer position")
	}
	if tb.start == 0 {
		return
	}
	copy(tb.buf, tb.buf[tb.start:size])
	tb.pos -= tb.start
	tb.offset += tb.start
	tb.start = 0
	return
}
