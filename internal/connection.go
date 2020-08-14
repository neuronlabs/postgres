package internal

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

// Connection is the interface for the connection.
type Connection interface {
	Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, values ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, values ...interface{}) pgx.Row
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
}

// Batch is the interface used for the batch queries.
type Batch interface {
	Len() int
	Queue(query string, arguments ...interface{})
}

var (
	_ Batch = &DummyBatch{}
	_ Batch = &pgx.Batch{}
)

type DummyBatch struct {
	Queries []*Query
}

func (d *DummyBatch) Len() int {
	return len(d.Queries)
}

func (d *DummyBatch) Queue(query string, arguments ...interface{}) {
	d.Queries = append(d.Queries, &Query{Query: query, Arguments: arguments})
}

// Query is a single query input for the DummyBatch.
type Query struct {
	Query     string
	Arguments []interface{}
}
