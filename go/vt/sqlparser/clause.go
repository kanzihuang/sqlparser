package sqlparser

import (
	"io"
	"strings"
)

//type Clause interface {
//	iClause()
//}
//
//var _ Clause = &SelectClause{}

type Clause struct {
	Name    string
	Expr    string
	Clauses []*Clause
}

//func (*SelectClause) iClause() {}

func NewClause(name, expr string) *Clause {
	return &Clause{
		Name: name,
		Expr: expr,
	}
}

type clauseTypes []int

var clsTypes = clauseTypes{
	SELECT,
	FROM,
}

func (types clauseTypes) Index(typ int) int {
	for i, val := range types {
		if val == typ {
			return i
		}
	}
	return -1
}

func scanClause(tokenizer *Tokenizer, name string) (*Clause, string, error) {
	var depth int
	var nextName string
	var sb strings.Builder
loop:
	for {
		tkn, val := tokenizer.Scan()
		switch tkn {
		case ';', 0, eofChar:
			break loop
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && clsTypes.Index(tkn) >= 0 {
				if len(name) == 0 {
					name = val
					tokenizer.resetCache()
					break
				}
				nextName = val
				tokenizer.resetCache()
				break loop
			}
			sb.WriteString(tokenizer.readCache())
		}
	}
	if tokenizer.LastError != nil {
		return nil, "", tokenizer.LastError
	}
	if sb.Len() == 0 {
		return nil, "", io.EOF
	}
	clause := NewClause(name, strings.TrimSpace(sb.String()))
	return clause, nextName, nil
}

// SplitStatementToClause returns the next sql statement or EOF.
func SplitStatementToClause(blob string) (*Clause, error) {
	//clauses := make([]Clause, 0, 8)
	var name string
	//var err error
	var root *Clause
	tokenizer := NewStringTokenizer(blob)
	//loop:
	for {
		//if len(name) == 0 {
		//	tkn, val := tokenizer.Scan()
		//	switch tkn {
		//	case ';', 0, eofChar:
		//		break loop
		//	default:
		//		if clsTypes.Index(tkn) < 0 {
		//			return nil, errors.New(fmt.Sprintf("syntax error at position %d near '%s'", tokenizer.buf.AbsolutePos()-len(val), val))
		//		}
		//		name = val
		//		tokenizer.resetCache()
		//	}
		//}
		//var nextName string
		clause, nextName, err := scanClause(tokenizer, name)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name = nextName
		if root == nil {
			root = clause
			continue
		}
		root.Clauses = append(root.Clauses, clause)
	}
	if tokenizer.LastError != nil {
		return nil, tokenizer.LastError
	}
	return root, nil
}
