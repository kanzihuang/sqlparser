# sqlparser

Go package for parsing MySQL SQL queries.

## Notice

The backbone of this repo is extracted from [vitessio/vitess](https://github.com/vitessio/vitess).

Inside vitessio/vitess there is a very nicely written sql parser. However, as it's not a self-contained application and does not support Windows, I created this one.
It applies the same LICENSE as vitessio/vitess.

The README.md is extracted from [xwb1989/sqlparser](https://github.com/xwb1989/sqlparser).

Inside xwb1989/sqlparser there is a very nicely written sql parser. However, it's not support current_timestamp().

## Usage

```go
import (
    "github.com/kanzihuang/go/vt/sqlparser"
)
```

Then use:

```go
sql := "SELECT * FROM table WHERE a = 'abc'"
stmt, err := sqlparser.Parse(sql)
if err != nil {
	// Do something with the err
}

// Otherwise do something with stmt
switch stmt := stmt.(type) {
case *sqlparser.Select:
	_ = stmt
case *sqlparser.Insert:
}
```

Alternative to read many queries from a io.Reader:

```go
r := strings.NewReader("INSERT INTO table1 VALUES (1, 'a'); INSERT INTO table2 VALUES (3, 4);")

tokens := sqlparser.NewReaderTokenizer(r)
for {
	stmt, err := sqlparser.ParseNext(tokens)
	if err == io.EOF {
		break
	}
	// Do something with stmt or err.
}
```

See [parse_test.go](https://github.com/kanzihuang/vitess/blob/sqlparser/go/vt/sqlparser/parse_test.go) for more examples, or read the [godoc](https://pkg.go.dev/github.com/kanzihuang/vitess/go/vt/sqlparser).
