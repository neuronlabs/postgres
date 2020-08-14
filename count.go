package postgres

import (
	"context"
	"strings"

	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/query"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/filters"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

// Count implements query.Counter interface.
func (p *Postgres) Count(ctx context.Context, s *query.Scope) (int64, error) {
	q, err := p.parseCountQuery(s)
	if err != nil {
		return 0, nil
	}
	if log.Level().IsAllowed(log.LevelDebug2) {
		log.Debug2f("[COUNT][QUERY] %s [VALUES]: %v", q.query, q.values)
	}

	row := p.connection(s).QueryRow(ctx, q.query, q.values...)
	var count int64
	if err := row.Scan(&count); err != nil {
		log.Debug2f("Scanning count value failed: %v", err)
		return 0, errors.WrapDetf(p.neuronError(err), "Scanning count failed - %v", err)
	}
	return count, nil
}

func (p *Postgres) parseCountQuery(s *query.Scope) (*simpleQuery, error) {
	sb := &strings.Builder{}
	sb.WriteString("SELECT COUNT(DISTINCT ")

	// get primary database name
	mStruct := s.ModelStruct
	p.writeQuotedWord(sb, mStruct.Primary().DatabaseName)
	sb.WriteString(") FROM ")
	p.writeQuotedWord(sb, mStruct.DatabaseSchemaName)
	sb.WriteRune('.')
	p.writeQuotedWord(sb, mStruct.DatabaseName)

	// Handle filters
	parsedFilters, err := filters.ParseFilters(s, p.writeQuotedWord)
	if err != nil {
		return nil, err
	}

	q := &simpleQuery{}

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
	q.query = sb.String()
	return q, nil
}
