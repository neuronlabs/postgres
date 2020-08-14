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

// // TestIntegrationPatch integration tests for update method.
func TestUpdate(t *testing.T) {
	c := testingController(t, true, &tests.SimpleModel{})
	p := testingRepository(c)

	ctx := context.Background()

	err := c.MigrateModels(ctx, &tests.SimpleModel{})
	require.NoError(t, err)

	mStruct, err := c.ModelStruct(&tests.SimpleModel{})
	require.NoError(t, err)

	defer func() {
		_ = internal.DropTables(ctx, p.ConnPool, mStruct.DatabaseName, mStruct.DatabaseSchemaName)
	}()

	// No results should return no error.
	db := database.New(c)

	newModel := func() *tests.SimpleModel {
		return &tests.SimpleModel{
			Attr: "Something",
		}
	}

	// Insert two models.
	model1 := newModel()
	model2 := newModel()
	err = db.Query(mStruct, model1, model2).Insert()
	require.NoError(t, err)

	t.Run("Model", func(t *testing.T) {
		model := newModel()
		model.ID = model1.ID
		model.Attr = "Other"
		affected, err := db.Query(mStruct, model).Update()
		require.NoError(t, err)

		assert.Equal(t, int64(1), affected)

		models, err := db.Query(mStruct).Where("ID =", model1.ID).Find()
		require.NoError(t, err)
		if assert.Len(t, models, 1) {
			assert.Equal(t, "Other", models[0].(*tests.SimpleModel).Attr)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		model := newModel()
		model.ID = 1e8
		affected, err := db.Query(mStruct, model).Update()
		assert.Error(t, err)
		assert.True(t, errors.Is(err, query.ErrNoResult), "%v", err)
		assert.Equal(t, int64(0), affected)
	})

	t.Run("Filters", func(t *testing.T) {
		model := newModel()
		model.Attr = "Something"
		affected, err := db.Query(mStruct, model).Select(mStruct.MustFieldByName("Attr")).Where("ID in", model1.ID, model2.ID).Update()
		require.NoError(t, err)

		assert.Equal(t, int64(2), affected)
	})
}

// func TestIntegrationPatch(t *testing.T) {
// 	c, db := prepareIntegrateRepository(t)
//
// 	defer db.Close()
// 	defer deleteTestModelTable(t, db)
//
// 	tm := &tests.Model{AttrString: "different"}
// 	s, err := query.NewC(c, tm)
// 	require.NoError(t, err)
//
// 	require.NoError(t, s.FilterField(query.NewFilterField(s.ModelStruct.Primary(), query.OpEqual, 2)))
// 	require.NoError(t, s.SetFields("AttrString"))
// 	if assert.NoError(t, s.Patch(), "%s", s) {
// 		ti := time.Time{}
// 		r := db.QueryRow("SELECT attr_string, updated_at FROM test_models WHERE id = $1", 2)
//
// 		var attr string
// 		require.NoError(t, r.Scan(&attr, &ti))
//
// 		assert.Equal(t, "different", attr)
//
// 		assert.Equal(t, ti.Round(time.Millisecond).Unix(), tm.UpdatedAt.Unix(), ti.String())
// 	}
// }
