package filters

import (
	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/query"
	"github.com/neuronlabs/neuron/query/filter"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

var (
	operatorSQL      []string
	operatorSQLizers []SQLizer
)

func init() {
	operatorSQL = make([]string, 16)
	operatorSQLizers = make([]SQLizer, 16)
	registerOperator(filter.OpEqual, BasicSQLizer, "=")
	registerOperator(filter.OpIn, InSQLizer, "IN")
	registerOperator(filter.OpNotIn, InSQLizer, "NOT IN")
	registerOperator(filter.OpNotEqual, BasicSQLizer, "<>")
	registerOperator(filter.OpGreaterEqual, BasicSQLizer, ">=")
	registerOperator(filter.OpGreaterThan, BasicSQLizer, ">")
	registerOperator(filter.OpLessEqual, BasicSQLizer, "<=")
	registerOperator(filter.OpLessThan, BasicSQLizer, "<")
	registerOperator(filter.OpContains, StringOperatorsSQLizer, "LIKE")
	registerOperator(filter.OpStartsWith, StringOperatorsSQLizer, "LIKE")
	registerOperator(filter.OpEndsWith, StringOperatorsSQLizer, "LIKE")
	registerOperator(filter.OpIsNull, NullSQLizer, "IS NULL")
	registerOperator(filter.OpNotNull, NullSQLizer, "IS NOT NULL")
}

// SQLQuery defines the SQL query Models pair
type SQLQuery struct {
	Query  string
	Values []interface{}
}

// SQLQueries is the wrapper arount the SQL queries value.
type SQLQueries []SQLQuery

// SQLizer is the function that sqlizes provided OperatorValuePair.
type SQLizer func(*query.Scope, internal.QuotedWordWriteFunc, filter.Simple) (SQLQueries, error)

// SQLOperator gets the operator sql name.
func SQLOperator(o *filter.Operator) (string, error) {
	return getSQLOperator(o)
}

// RegisterSQLizer registers new SQLizer function for the provided operator. Optionally can set the raw SQL value.
func RegisterSQLizer(o *filter.Operator, sqlizer SQLizer, raw ...string) {
	registerOperator(o, sqlizer, raw...)
}

/** PRIVATE */

func getSQLOperator(o *filter.Operator) (string, error) {
	if o == nil {
		log.Errorf("Provided nil filter operator.")
		return "", errors.WrapDet(filter.ErrFilterFormat, "provided nil operator")
	}
	if int(o.ID) > len(operatorSQL)-1 {
		log.Errorf("Cannot get filter operator: '%s' SQL ", o.Name)
		return "", errors.WrapDet(filter.ErrFilterFormat, "unsupported filter operator")
	}

	sql := operatorSQL[o.ID]
	if sql == "" {
		log.Errorf("Operator: '%s' has SQL value ", o.Name)
		return "", errors.WrapDetf(filter.ErrFilterFormat, "filter operator: '%v' hase no SQL value", o.Name)
	}

	return sql, nil
}

func getOperatorSQLizer(o *filter.Operator) (SQLizer, error) {
	if int(o.ID) > len(operatorSQLizers)-1 {
		return nil, errors.WrapDet(filter.ErrFilterFormat, "unsupported filter operator")
	}

	return operatorSQLizers[o.ID], nil
}

func registerOperator(o *filter.Operator, sqlizer SQLizer, raw ...string) {
	registerOperatorSQLizer(o, sqlizer)
	if len(raw) > 0 {
		registerOperatorRawSQL(o, raw[0])
	}
}

func registerOperatorSQLizer(o *filter.Operator, sqlizer SQLizer) {
	minSize := len(operatorSQLizers) - 1

	for int(o.ID) > minSize {
		if minSize == 0 {
			minSize = 1
		}
		minSize *= 2
	}

	if minSize != len(operatorSQLizers)-1 {
		temp := make([]SQLizer, minSize)
		copy(temp, operatorSQLizers)
		operatorSQLizers = temp
	}

	operatorSQLizers[o.ID] = sqlizer
}

func registerOperatorRawSQL(o *filter.Operator, raw string) {
	minSize := len(operatorSQL) - 1

	for int(o.ID) > minSize {
		if minSize == 0 {
			minSize = 1
		}
		minSize *= 2
	}

	if minSize != len(operatorSQL)-1 {
		temp := make([]string, minSize)
		copy(temp, operatorSQL)
		operatorSQL = temp
	}

	operatorSQL[o.ID] = raw
}
