// +build integrate

package migrate

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
)

// TestKeyWords tests the keywords functions.
func TestKeyWords(t *testing.T) {
	cfg := internal.TestingPostgresConfig(t)

	ctx := context.Background()

	conn, err := pgxpool.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close()

	// Get Version gets postgres version.
	version, err := GetVersion(ctx, conn)
	require.NoError(t, err)

	require.NotEqual(t, 0, version)

	_, err = GetKeyWords(ctx, conn, version)
	require.NoError(t, err)
}
