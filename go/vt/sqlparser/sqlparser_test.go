package sqlparser

import (
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
)

func Test_ParseNext(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
		err   string
	}{
		{
			name:  "create table `my-table`",
			input: "create table `my-table` (`my-id` bigint(20))",
			want:  []string{"create table `my-table` (\n\t`my-id` bigint(20)\n)"},
		},
		{
			name:  "create table \"my-table\"",
			input: "create table \"my-table\" (\"my-id\" bigint(20))",
			want:  []string{"create table \"my-table\" (\n\t\"my-id\" bigint(20)\n)"},
			err:   "near 'my-table'",
		},
		{
			name:  "create table with current_timestamp",
			input: "create table tbl (id bigint(20), create_at datetime DEFAULT current_timestamp())",
			want:  []string{"create table tbl (\n\tid bigint(20),\n\tcreate_at datetime default current_timestamp()\n)"},
		},
		{
			name:  "select concat()",
			input: "select concat(c1,'�'), concat('�',c1) from t1;",
			want:  []string{"select concat(c1, '�'), concat('�', c1) from t1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tokens := NewReaderTokenizer(strings.NewReader(test.input))

			for _, want := range test.want {
				tree, err := ParseNext(tokens)
				if len(test.err) > 0 {
					require.Error(t, err)
					require.Contains(t, err.Error(), test.err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, want, String(tree))
			}

			// Read once more and it should be EOF.
			if _, err := ParseNext(tokens); err != io.EOF {
				require.Equal(t, test.err, err)
			}

			// And again, once more should be EOF.
			if _, err := ParseNext(tokens); err != io.EOF {
				require.Equal(t, test.err, err)
			}
		})
	}
}

func Test_ParseNext_LongString(t *testing.T) {
	const times = 0x1000
	sb := strings.Builder{}
	sb.WriteString("select '")
	for i := 0; i < times; i++ {
		sb.WriteString("0123456789")
	}
	sb.WriteString("' from tbl")
	token := NewReaderTokenizer(strings.NewReader(sb.String()))
	stmt, err := ParseNext(token)
	require.NoError(t, err)
	require.Equal(t, sb.String(), String(stmt))
}
