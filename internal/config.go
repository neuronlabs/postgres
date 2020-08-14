package internal

import (
	"os"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron/repository"
)

// PoolConfig gets the pool configuration from the provided repository options.
func PoolConfig(options *repository.Options) (*pgxpool.Config, error) {
	if options.URI != "" {
		return pgxpool.ParseConfig(options.URI)
	}
	return &pgxpool.Config{ConnConfig: &pgx.ConnConfig{Config: pgconn.Config{
		Host:      options.Host,
		Port:      options.Port,
		Database:  options.Database,
		User:      options.Username,
		Password:  options.Password,
		TLSConfig: options.TLSConfig,
	}}}, nil
}

// TestingPostgresConfig gets postgres config from the POSTGRES_TESTING environment variable.
func TestingPostgresConfig(t testing.TB) *pgxpool.Config {
	pg, ok := os.LookupEnv("POSTGRES_TESTING")
	if !ok {
		t.Skip("POSTGRES_TESTING environment variable not defined")
	}

	cfg, err := pgxpool.ParseConfig(pg)
	require.NoError(t, err)
	return cfg
}
