package buffer

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func Test_Peek(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		buf  []byte
		pos  int
		peek int
		want uint16
	}{
		{
			name: "first peek",
			sql:  "01234567890123456789",
			buf:  nil,
			want: '0',
		},
		{
			name: "pos 5, peek 0",
			sql:  "01234567890123456789",
			buf:  []byte("abcdefghij"),
			pos:  5,
			peek: 0,
			want: 'f',
		},
		{
			name: "pos 5, peek 1",
			sql:  "01234567890123456789",
			buf:  []byte("abcdefghij"),
			pos:  5,
			peek: 6,
			want: '1',
		},
		{
			name: "pos 10, peek 3",
			sql:  "01234567890123456789",
			buf:  []byte("abcdefghij"),
			pos:  10,
			peek: 2,
			want: '2',
		},
		{
			name: "with some data and EOF",
			sql:  "012345",
			buf:  []byte("abcdefghij"),
			pos:  10,
			peek: 5,
			want: '5',
		},
		{
			name: "peek with empty",
			sql:  "",
			buf:  []byte("abcdefghij"),
			pos:  10,
			peek: 0,
			want: eofChar,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := Buffer{
				reader: strings.NewReader(tt.sql),
				buf:    tt.buf,
				start:  tt.pos,
				pos:    tt.pos,
			}
			got := buf.Peek(tt.peek)
			require.Equal(t, tt.want, got)
		})
	}
}

//func Test_buffer_reset(t *testing.T) {
//	tests := []struct {
//		name string
//		buf  []byte
//		size int
//		pos  int
//		want string
//	}{
//		{
//			name: "rest 2:5",
//			buf:  []byte("abcdefghij"),
//			size: 5,
//			pos:  2,
//			want: "cde",
//		},
//		{
//			name: "rest 5:5",
//			buf:  []byte("abcdefghij"),
//			size: 5,
//			pos:  5,
//			want: "",
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			buf := &Buffer{
//				buf:   tt.buf,
//				size:  tt.size,
//				start: tt.pos,
//			}
//			buf.reset()
//			require.Equal(t, tt.want, string(buf.buf[buf.start:buf.size]))
//		})
//	}
//}
