package filters

import (
	"strings"

	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/query"
	"github.com/neuronlabs/neuron/query/filter"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

// ParseFilters parses the filters into SQLQueries for the provided scope.
func ParseFilters(s *query.Scope, writer internal.QuotedWordWriteFunc) (SQLQueries, error) {
	queries := SQLQueries{}

	// at first get primary filters
	for _, scopeFilter := range s.Filters {
		switch ft := scopeFilter.(type) {
		case filter.Simple:
			if ft.StructField.DatabaseSkip() {
				log.Debug2f("Skipping foreign key filter with db:\"-\" omit option")
				continue
			}
			sqlizer, err := getOperatorSQLizer(ft.Operator)
			if err != nil {
				err := errors.WrapDet(filter.ErrFilterFormat, "unsupported filter operator").
					WithDetailf("Provided unsupported operator: '%s' for given query.", ft.Operator.Name)
				return nil, err
			}
			subQueries, err := sqlizer(s, writer, ft)
			if err != nil {
				return nil, err
			}
			queries = append(queries, subQueries...)
		case filter.OrGroup:
			var (
				orQueries SQLQueries
				sb        strings.Builder
			)
			for _, elem := range ft {
				if elem.StructField.DatabaseSkip() {
					log.Debug2f("Skipping foreign key filter with db:\"-\" omit option")
					continue
				}
				sqlizer, err := getOperatorSQLizer(elem.Operator)
				if err != nil {
					err := errors.WrapDet(filter.ErrFilterFormat, "unsupported filter operator").
						WithDetailf("Provided unsupported operator: '%s' for given query.", elem.Operator.Name)
					return nil, err
				}
				subQueries, err := sqlizer(s, writer, elem)
				if err != nil {
					return nil, err
				}
				orQueryElem := SQLQuery{}
				if len(subQueries) > 1 {
					sb.WriteRune('(')
				}
				for i, f := range subQueries {
					sb.WriteString(f.Query)
					if i < len(subQueries)-1 {
						sb.WriteString(" AND ")
					}
					orQueryElem.Values = append(orQueryElem.Values, f.Values...)
				}
				if len(subQueries) > 1 {
					sb.WriteRune(')')
				}
				orQueryElem.Query = sb.String()
				sb.Reset()
				orQueries = append(orQueries, orQueryElem)
			}

			if len(orQueries) > 1 {
				orQuery := SQLQuery{}
				sb.WriteRune('(')
				for i, orQueryElem := range orQueries {
					sb.WriteString(orQueryElem.Query)
					if i < len(orQueries)-1 {
						sb.WriteString(" OR ")
					}
					orQuery.Values = append(orQuery.Values, orQueryElem.Values...)
				}
				sb.WriteRune(')')
				orQuery.Query = sb.String()
				queries = append(queries, orQuery)
			} else {
				// 0 or 1 filter.
				queries = append(queries, orQueries...)
			}
		default:
			continue
		}
	}
	return queries, nil
}
