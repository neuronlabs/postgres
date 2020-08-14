// +build integrate

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/tests"
	"github.com/neuronlabs/neuron/database"
	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/query"
)

func TestTransactions(t *testing.T) {
	c := testingController(t, true, testModels...)
	p := testingRepository(c)

	ctx := context.Background()

	mStruct, err := c.ModelStruct(&tests.SimpleModel{})
	require.NoError(t, err)

	defer func() {
		_ = internal.DropTables(ctx, p.ConnPool, mStruct.DatabaseName, mStruct.DatabaseSchemaName)
	}()

	db := database.New(c)

	t.Run("Commit", func(t *testing.T) {
		// No results should return no error.
		tx := db.Begin(ctx, nil)

		model := &tests.SimpleModel{Attr: "Name"}
		err = tx.Query(mStruct, model).Insert()
		require.NoError(t, err)

		_, ok := p.transactions[tx.Transaction.ID]
		assert.True(t, ok)

		assert.NotEqual(t, 0, model.ID)
		err = tx.Commit()
		require.NoError(t, err)
		_, ok = p.transactions[tx.Transaction.ID]
		assert.False(t, ok)

		res, err := db.Query(mStruct).Where("id =", model.ID).Get()
		require.NoError(t, err)

		assert.Equal(t, res.GetPrimaryKeyValue(), model.ID)
	})

	t.Run("Rollback", func(t *testing.T) {
		// No results should return no error.
		tx := db.Begin(ctx, nil)

		model := &tests.SimpleModel{Attr: "Name"}
		err = tx.Query(mStruct, model).Insert()
		require.NoError(t, err)

		_, ok := p.transactions[tx.Transaction.ID]
		assert.True(t, ok)

		assert.NotEqual(t, 0, model.ID)

		err = tx.Rollback()
		require.NoError(t, err)

		_, ok = p.transactions[tx.Transaction.ID]
		assert.False(t, ok)

		_, err := db.Query(mStruct).Where("id =", model.ID).Get()
		require.Error(t, err)
		assert.True(t, errors.Is(err, query.ErrNoResult))
	})
}
