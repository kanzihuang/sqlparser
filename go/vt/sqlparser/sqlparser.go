package sqlparser

func (tkn *Tokenizer) cur() uint16 {
	return tkn.buf.Cur()
}

func (tkn *Tokenizer) skip(dist int) {
	tkn.buf.Skip(dist)
}

func (tkn *Tokenizer) peek(dist int) uint16 {
	return tkn.buf.Peek(dist)
}

func (tkn *Tokenizer) back(dist int) {
	tkn.buf.Back(dist)
}

func (tkn *Tokenizer) next() {
	tkn.buf.Next()
}

func (tkn *Tokenizer) read() string {
	return tkn.buf.Read()
}

func (tkn *Tokenizer) absolutePos() int {
	return tkn.buf.AbsolutePos()
}

func (tkn *Tokenizer) absoluteStart() int {
	return tkn.buf.AbsoluteStart()
}
