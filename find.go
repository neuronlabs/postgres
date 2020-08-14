package postgres

import (
	"context"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"

	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/mapping"
	"github.com/neuronlabs/neuron/query"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/filters"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

// Find lists all the values that matches scope's filters, sorts and pagination.
// Implements repository.Repository interface.
func (p *Postgres) Find(ctx context.Context, s *query.Scope) error {
	q, err := p.parseSelectQuery(s)
	if err != nil {
		log.Debug2("parse Select query failed: %v", err)
		return err
	}

	rows, err := p.connection(s).Query(ctx, q.query, q.values...)
	if err != nil {
		return err
	}
	defer func() {
		rows.Close()
	}()

	for rows.Next() {
		if err := p.scanRow(s, q, rows); err != nil {
			return errors.Wrapf(p.neuronError(err), "scanning row failed: %v", err)
		}
	}
	return nil
}

func (p *Postgres) scanRow(s *query.Scope, q *selectQuery, rows pgx.Rows) (err error) {
	model := mapping.NewModel(s.ModelStruct)
	var (
		fieldValues  []interface{}
		fieldValue   interface{}
		timePointers []int
	)
	fielder, ok := model.(mapping.Fielder)
	if !ok {
		return errors.Wrapf(mapping.ErrModelNotImplements, "Model: '%s' doesn't implement Fielder interface", s.ModelStruct)
	}

	// get the field values with the provided order
	for i, field := range q.fieldsOrder {
		if field.IsTimePointer() {
			if log.Level() == log.LevelDebug3 {
				log.Debug3f("scanned Field: '%s' isTimePointer", field.Name())
			}
			timePointers = append(timePointers, i)
			fieldValues = append(fieldValues, &pgtype.Timestamp{})
		} else {
			if log.Level() == log.LevelDebug3 {
				log.Debug3f("scanned Field: '%s'", field.ReflectField().Type)
			}
			fieldValue, err = fielder.GetFieldsAddress(field)
			if err != nil {
				return err
			}
			fieldValues = append(fieldValues, fieldValue)
		}
	}

	// Scan models value.
	if err := rows.Scan(fieldValues...); err != nil {
		return err
	}

	// Set time pointers.
	for _, index := range timePointers {
		nt, ok := fieldValues[index].(*pgtype.Timestamp)
		if !ok {
			log.Errorf("Getting NullTime failed. ")
			continue
		}
		if nt.Status != pgtype.Null {
			err = fielder.SetFieldValue(q.fieldsOrder[index], nt.Time)
		} else {
			err = fielder.SetFieldZeroValue(q.fieldsOrder[index])
		}
		if err != nil {
			return err
		}
	}
	s.Models = append(s.Models, model)
	return nil
}

type selectQuery struct {
	query       string
	values      []interface{}
	fieldsOrder []*mapping.StructField
}

func (p *Postgres) parseSelectQuery(s *query.Scope) (*selectQuery, error) {
	commonFieldSet, hasCommonFieldset := s.CommonFieldSet()
	if !hasCommonFieldset {
		return nil, errors.Wrap(query.ErrNoFieldsInFieldSet, "no fieldset found for the list/get type query")
	}

	var fields string
	q := &selectQuery{}
	sb := &strings.Builder{}

	var i int
	for _, field := range commonFieldSet {
		if field.DatabaseSkip() {
			continue
		}

		q.fieldsOrder = append(q.fieldsOrder, field)
		p.writeQuotedWord(sb, field.DatabaseName)
		if i != len(commonFieldSet)-1 {
			sb.WriteString(", ")
		}
		i++
	}
	fields = sb.String()
	if fields == "" {
		// All the fields had to be omitted.
		return nil, errors.Wrap(query.ErrNoFieldsInFieldSet, "provided empty fieldset for the list/get query")
	}
	sb.Reset()

	mStruct := s.ModelStruct
	// Prepare the select query for given fields.
	sb.WriteString("SELECT ")
	sb.WriteString(fields)
	sb.WriteString(" FROM ")
	p.writeQuotedWord(sb, mStruct.DatabaseSchemaName)
	sb.WriteRune('.')
	p.writeQuotedWord(sb, mStruct.DatabaseName)

	// Parse filters and store in the string builder.
	parsedFilters, err := filters.ParseFilters(s, p.writeQuotedWord)
	if err != nil {
		return nil, err
	}

	if len(parsedFilters) > 0 {
		sb.WriteString(" WHERE ")
		for i, f := range parsedFilters {
			sb.WriteString(f.Query)
			if i < len(parsedFilters)-1 {
				sb.WriteString(" AND ")
			}
			q.values = append(q.values, f.Values...)
		}
	}

	err = p.parseSelectSort(s, sb)
	if err != nil {
		return nil, err
	}

	paginationValues := parseSelectPagination(s, sb)
	if paginationValues != nil {
		q.values = append(q.values, paginationValues...)
	}

	q.query = sb.String()
	return q, nil
}

func parseSelectPagination(s *query.Scope, sb *strings.Builder) (values []interface{}) {
	if s.Pagination == nil {
		return values
	}
	if s.Pagination.Limit != 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(internal.StringIncrementor(s))
		values = append(values, s.Pagination.Limit)
	}

	if s.Pagination.Offset != 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(internal.StringIncrementor(s))
		values = append(values, s.Pagination.Offset)
	}
	return values
}

func (p *Postgres) parseSelectSort(s *query.Scope, sb *strings.Builder) error {
	if log.Level() == log.LevelDebug3 {
		log.Debug3f("[SCOPE][%s] sorting fields: %v", s.ID, s.SortingOrder)
	}
	if len(s.SortingOrder) == 0 {
		return nil
	}

	sb.WriteString(" ORDER BY ")
	for i, field := range s.SortingOrder {
		if log.Level() == log.LevelDebug3 {
			log.Debug3f("Sorting by field: '%s' with '%s' order", field.Field().NeuronName(), field.Order().String())
		}
		p.writeQuotedWord(sb, field.Field().DatabaseName)

		if field.Order() == query.DescendingOrder {
			log.Debug2f("[SCOPE][%s] descending sorting by: '%s' at: '%d' sort order", s.ID, field.Field().DatabaseName, i)
			sb.WriteString(" DESC")
		} else {
			log.Debug2f("[SCOPE][%s] ascending sorting by: '%s' at: '%d' sort order", s.ID, field.Field().DatabaseName, i)
			sb.WriteString(" ASC")
		}
		if i != len(s.SortingOrder)-1 {
			sb.WriteString(", ")
		}
	}
	return nil
}
