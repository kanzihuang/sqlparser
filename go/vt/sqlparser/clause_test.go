package sqlparser

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// TestSplitNextEdgeCases tests various SplitNext edge cases.
func TestSplitStatementToClauses(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *Clause
	}{
		{
			input: "select 1",
			want: &Clause{
				Name: "select",
				Expr: "1",
			},
		},
		//{
		//	input: "select 1; select 2",
		//	want: &SelectClause{
		//		Expr: "1",
		//	},
		//},
		//{
		//	input: "(((select 1)))",
		//	want: &SelectClause{
		//		Expr: "1",
		//	},
		//},
		//{
		//	input: "(((select 1)) union (select 2) union select 3) limit 1",
		//	want: []SelectClause{
		//		{
		//			Expr: "1",
		//		},
		//	},
		//},
		//{
		//	input: "select a, b from tbl1, tbl2;",
		//	want: []SelectClause{
		//		{
		//			value: "a, b",
		//		},
		//		{
		//			name:  "from",
		//			value: "tbl1, tbl2",
		//		},
		//	},
		//},
		//{
		//	input: "select 1 from (select 1) as `my-table`;",
		//	want: []SelectClause{
		//		{
		//			name:  "select",
		//			value: "1",
		//		},
		//		{
		//			name:  "from",
		//			value: "(select 1) as `my-table`",
		//		},
		//	},
		//},
		//{
		//	input: "select 1 union select 2;",
		//	want: []SelectClause{
		//		{
		//			name:  "select",
		//			value: "1",
		//		},
		//		{
		//			name:  "from",
		//			value: "(select 1) as `my-table`",
		//		},
		//	},
		//},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			Parse(test.input)
			clauses, err := SplitStatementToClause(test.input)
			require.NoError(t, err)
			require.Equal(t, test.want, clauses)
		})
	}
}
