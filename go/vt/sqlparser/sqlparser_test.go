package sqlparser

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
)

func Test_ParseNext(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
		err   string
	}{
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
			tokens := NewReaderTokenizer(strings.NewReader(test.input))
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

func Test_SplitNext(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{{
		input:  "select * from `my-table`; \t; \n; \n\t\t ;select * from `my-table`;",
		output: "select * from `my-table`;select * from `my-table`",
	}, {
		input: "select * from `my-table`",
	}, {
		input:  "select * from `my-table`;",
		output: "select * from `my-table`",
	}, {
		input:  "select * from `my-table`;   ",
		output: "select * from `my-table`",
	}, {
		input:  "select * from `my-table1`; select * from `my-table2`;",
		output: "select * from `my-table1`; select * from `my-table2`",
	}, {
		input:  "select * from /* comment ; */ table;",
		output: "select * from /* comment ; */ table",
	}, {
		input:  "select * from `my-table` where semi = ';';",
		output: "select * from `my-table` where semi = ';'",
	}, {
		input:  "select * from `my-table1`;-- comment;\nselect * from `my-table2`;",
		output: "select * from `my-table1`;-- comment;\nselect * from `my-table2`",
	}, {
		input: "CREATE TABLE `total_data` (`id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id', " +
			"`region` varchar(32) NOT NULL COMMENT 'region name, like zh; th; kepler'," +
			"`data_size` bigint NOT NULL DEFAULT '0' COMMENT 'data size;'," +
			"`createtime` datetime NOT NULL DEFAULT NOW() COMMENT 'create time;'," +
			"`comment` varchar(100) NOT NULL DEFAULT '' COMMENT 'comment'," +
			"PRIMARY KEY (`id`))",
	}, {
		input: "create table \"my-table\" (\"my-id\" bigint(20))",
	}}

	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}
			tokenizer := NewReaderTokenizer(strings.NewReader(tcase.input), WithBufferCache())
			var sb strings.Builder
			for {
				stmt, err := SplitNext(tokenizer)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				sb.WriteString(stmt)
				sb.WriteString(";")
			}
			got := sb.String()
			got = got[:len(got)-1]
			require.Equal(t, tcase.output, got)
		})
	}
}
