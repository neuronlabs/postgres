package filters

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron/mapping"
	"github.com/neuronlabs/neuron/query"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/migrate"
)

//go:generate neurogns models methods methods --format=goimports --type=QueryModel --single-file .

// queryModel is the model used for testing the queries filters.
type QueryModel struct {
	ID         int    `neuron:"type=primary"`
	StringAttr string `neuron:"type=attr"`
}

func getScope(t *testing.T) *query.Scope {
	t.Helper()

	m := mapping.NewModelMap(mapping.WithNamingConvention(mapping.SnakeCase))

	err := m.RegisterModels(&QueryModel{})
	require.NoError(t, err)

	mStruct, ok := m.GetModelStruct(&QueryModel{})
	require.True(t, ok)

	err = migrate.PrepareModels(mStruct)
	require.NoError(t, err)

	return query.NewScope(mStruct)
}
