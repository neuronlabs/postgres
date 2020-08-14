package postgres

import (
	"context"
	"strings"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/filters"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/query"
)

// Delete deletes all the values that matches scope's filters.
// Implements repository.Repository interface.
func (p *Postgres) Delete(ctx context.Context, s *query.Scope) (int64, error) {
	q, err := p.parseDeleteQuery(s)
	if err != nil {
		return 0, err
	}

	if log.Level().IsAllowed(log.LevelDebug2) {
		log.Debug2f("[DELETE] %s", q.query)
	}

	// Execute prepared query.
	res, err := p.connection(s).Exec(ctx, q.query, q.values...)
	if err != nil {
		return 0, errors.Wrap(p.neuronError(err), "delete query failed")
	}

	return res.RowsAffected(), nil
}

type simpleQuery struct {
	query  string
	values []interface{}
}

func (p *Postgres) parseDeleteQuery(s *query.Scope) (*simpleQuery, error) {
	var sb strings.Builder

	mStruct := s.ModelStruct
	sb.WriteString("DELETE FROM ")
	p.writeQuotedWord(&sb, mStruct.DatabaseSchemaName)
	sb.WriteRune('.')
	p.writeQuotedWord(&sb, mStruct.DatabaseName)

	parsedFilters, err := filters.ParseFilters(s, p.writeQuotedWord)
	if err != nil {
		return nil, err
	}

	q := &simpleQuery{}
	// check if there is any filter
	if len(parsedFilters) > 0 {
		sb.WriteString(" WHERE ")
		for i, sq := range parsedFilters {
			sb.WriteString(sq.Query)
			if i < len(parsedFilters)-1 {
				sb.WriteString(" AND ")
			}
			q.values = append(q.values, sq.Values...)
		}
	}
	q.query = sb.String()
	return q, nil
}
