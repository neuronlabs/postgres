package internal

var (
	// PostgresVersionKey is the query's store key used to set the postgres server version.
	PostgresVersionKey = pgversion{}
	// IncrementorKey is the scope's context key used to save current incrementor value.
	IncrementorKey = incrementorKey{}
)

type pgversion struct{}
type incrementorKey struct{}
