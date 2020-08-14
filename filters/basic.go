package filters

import (
	"strings"

	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/query"
	"github.com/neuronlabs/neuron/query/filter"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
)

// BasicSQLizer gets the SQLQueries from the provided filter.
func BasicSQLizer(s *query.Scope, quotedWriter internal.QuotedWordWriteFunc, simple filter.Simple) (SQLQueries, error) {
	queries := SQLQueries{}

	op, err := getSQLOperator(simple.Operator)
	if err != nil {
		return nil, err
	}
	b := &strings.Builder{}
	for _, v := range simple.Values {
		quotedWriter(b, simple.StructField.DatabaseName)
		b.WriteString(" ")
		b.WriteString(op)
		b.WriteString(" ")
		b.WriteString(internal.StringIncrementor(s))
		queries = append(queries, SQLQuery{Query: b.String(), Values: []interface{}{v}})
		b.Reset()
	}
	return queries, nil
}

// NullSQLizer is the SQLizer function that returns NULL'ed queries.
func NullSQLizer(_ *query.Scope, quotedWriter internal.QuotedWordWriteFunc, simple filter.Simple) (SQLQueries, error) {
	op, err := getSQLOperator(simple.Operator)
	if err != nil {
		return nil, err
	}

	b := &strings.Builder{}
	quotedWriter(b, simple.StructField.DatabaseName)
	b.WriteString(" ")
	b.WriteString(op)

	queries := SQLQueries{SQLQuery{Query: b.String()}}

	return queries, nil
}

// InSQLizer creates the SQLQueries for the 'IN' and 'NOT IN' filter Operators.
func InSQLizer(s *query.Scope, quotedWriter internal.QuotedWordWriteFunc, simple filter.Simple) (SQLQueries, error) {
	if simple.Values == nil || len(simple.Values) == 0 {
		return SQLQueries{}, nil
	}
	op, err := getSQLOperator(simple.Operator)
	if err != nil {
		return nil, err
	}

	b := &strings.Builder{}

	quotedWriter(b, simple.StructField.DatabaseName)
	b.WriteString(" ")
	b.WriteString(op)
	b.WriteString(" (")

	for i := range simple.Values {
		b.WriteString(internal.StringIncrementor(s))
		if i != len(simple.Values)-1 {
			b.WriteRune(',')
		}
	}
	b.WriteRune(')')

	queries := SQLQueries{SQLQuery{Query: b.String(), Values: simple.Values}}

	return queries, nil
}

// StringOperatorsSQLizer creates the SQLQueries for the provided filter values.
func StringOperatorsSQLizer(s *query.Scope, quotedWriter internal.QuotedWordWriteFunc, simple filter.Simple) (SQLQueries, error) {
	op, err := getSQLOperator(simple.Operator)
	if err != nil {
		return nil, err
	}

	queries := SQLQueries{}

	b := &strings.Builder{}
	for _, v := range simple.Values {
		strValue, ok := v.(string)
		if !ok {
			return nil, errors.WrapDetf(filter.ErrFilterValues, "operator: '%s' requires string filter values", simple.Operator.Name)
		}

		switch simple.Operator {
		case filter.OpStartsWith:
			strValue += "%"
		case filter.OpEndsWith:
			strValue = "%" + strValue
		case filter.OpContains:
			strValue = "%" + strValue + "%"
		}

		quotedWriter(b, simple.StructField.DatabaseName)
		b.WriteRune(' ')
		b.WriteString(op)
		b.WriteRune(' ')
		b.WriteString(internal.StringIncrementor(s))

		queries = append(queries, SQLQuery{Query: b.String(), Values: []interface{}{strValue}})
		b.Reset()
	}

	return queries, nil
}
