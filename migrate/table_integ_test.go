// +build integrate

package migrate

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron/mapping"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

var cfg pgxpool.Config

// TestAutoMigrateModels tests the auto migration of the models
func TestAutoMigrateModels(t *testing.T) {
	repoCfg := internal.TestingPostgresConfig(t)
	models := []mapping.Model{&Model{}, &BasicModel{}}

	m := tCtrl(t, models...)

	log.SetLevel(log.LevelDebug)

	ctx := context.Background()
	db, err := pgxpool.ConnectConfig(ctx, repoCfg)
	require.NoError(t, err)

	defer db.Close()

	for _, model := range models {
		modelStruct, ok := m.GetModelStruct(model)
		require.True(t, ok)

		err := Models(ctx, db, modelStruct)
		require.NoError(t, err)

		_, err = db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", quoteIdentifier(modelStruct.DatabaseSchemaName), modelStruct.DatabaseName))
		if err != nil {
			log.Debugf("Error while dropping table: %v", err)
		}
	}
}
