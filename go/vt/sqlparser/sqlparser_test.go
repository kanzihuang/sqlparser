package sqlparser

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func Test_ParseNext(t *testing.T) {
	tests := []struct {
		name    string
		dialect Dialect
		input   string
		want    string
		err     string
	}{
		{
			name:    "mysql select '\\\\\\'hello'''",
			input:   "select '\\\\\\'hello''' from dual",
			want:    "select '\\\\\\'hello\\'' from dual",
			dialect: MysqlDialect{},
		},
		{
			name:    "postgres select '\\''hello'''",
			input:   "select '\\''hello''' from dual",
			want:    "select '\\\\\\'\\'hello\\'\\'' from dual",
			dialect: PostgresDialect{},
		},
		{
			name:  "create table `my-table`",
			input: "create table `my-table` (\n\t`my-id` bigint(20)\n)",
		},
		{
			name:  "create table \"my-table\"",
			input: "create table \"my-table\" (\"my-id\" bigint(20))",
			err:   "near 'my-table'",
		},
		{
			name:  "select concat()",
			input: "select concat(c1, '�'), concat('�', c1) from t1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.dialect == nil {
				test.dialect = MysqlDialect{}
			}
			tokens := NewReaderTokenizer(strings.NewReader(test.input), WithDialect(test.dialect))
			tree, err := ParseNext(tokens)
			if len(test.err) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.err)
				return
			}
			require.NoError(t, err)
			if len(test.want) == 0 {
				test.want = test.input
			}
			require.Equal(t, test.want, String(tree))
		})
	}
}

func Test_ParseNext_WithCurrentTime(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
		err   string
	}{
		{
			name:  "select %s",
			input: "select %s() from dual",
		},
		{
			name:  "update with %s",
			input: "update tbl set create_at = %s()",
		},
		{
			name:  "create table with %s",
			input: "create table tbl (\n\tcreate_at datetime default %s()\n)",
		},
	}
	items := []string{
		"current_timestamp",
		"localtime",
		"localtimestamp",
		"utc_timestamp",
		"now",
		"sysdate",
		"current_date",
		"curdate",
		"utc_time",
		"current_time",
		"curtime",
	}
	for _, test := range tests {
		for _, item := range items {
			name := fmt.Sprintf(test.name, item)
			input := fmt.Sprintf(test.input, item)
			want := input
			if len(test.want) > 0 {
				want = fmt.Sprintf(test.want, item)
			}
			t.Run(name, func(t *testing.T) {
				tokens := NewReaderTokenizer(strings.NewReader(input))
				tree, err := ParseNext(tokens)
				require.NoError(t, err)
				require.Equal(t, want, String(tree))
			})
		}
	}
}

func Test_ParseNext_WithLongString(t *testing.T) {
	const times = 0x1000
	sb := strings.Builder{}
	sb.WriteString("select '")
	for i := 0; i < times; i++ {
		sb.WriteString("0123456789")
	}
	sb.WriteString("', '\\n")
	for i := 0; i < times; i++ {
		sb.WriteString("0123456789")
	}
	sb.WriteString("' from tbl")
	token := NewReaderTokenizer(strings.NewReader(sb.String()))
	require.NotPanics(t, func() {
		stmt, err := ParseNext(token)
		require.NoError(t, err)
		require.Equal(t, sb.String(), String(stmt))
	})
}
