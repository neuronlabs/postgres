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
)

// TestRepositoryFind tests the repository list method.
func TestRepositoryFind(t *testing.T) {
	c := testingController(t, true, testModels...)
	p := testingRepository(c)

	ctx := context.Background()

	mStruct, err := c.ModelStruct(&tests.SimpleModel{})
	require.NoError(t, err)

	defer func() {
		_ = internal.DropTables(ctx, p.ConnPool, mStruct.DatabaseName, mStruct.DatabaseSchemaName)
	}()

	// No results should return no error.
	db := database.New(c)

	models, err := db.Query(mStruct).Find()
	require.NoError(t, err)

	assert.Len(t, models, 0)

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

	t.Run("Limit", func(t *testing.T) {
		models, err = db.Query(mStruct).Limit(1).Find()
		require.NoError(t, err)

		if assert.Len(t, models, 1) {
			assert.Equal(t, models[0].GetPrimaryKeyValue(), model1.ID)
		}
	})

	t.Run("Offset", func(t *testing.T) {
		models, err = db.Query(mStruct).Offset(1).Find()
		require.NoError(t, err)

		if assert.Len(t, models, 1) {
			assert.Equal(t, models[0].GetPrimaryKeyValue(), model2.ID)
		}
	})

	t.Run("Filter", func(t *testing.T) {
		models, err = db.Query(mStruct).Where("ID =", model2.ID).Find()
		require.NoError(t, err)

		if assert.Len(t, models, 1) {
			assert.Equal(t, models[0].GetPrimaryKeyValue(), model2.ID)
		}
	})
}

// func TestRepositoryList(t *testing.T) {
// 	c, db := prepareIntegrateRepository(t)
//
// 	defer db.Close()
// 	defer deleteTestModelTable(t, db)
//
// 	t.Run("Valid", func(t *testing.T) {
// 		t.Run("#1", func(t *testing.T) {
// 			s, err := query.NewC(c, &[]*tests.Model{})
// 			require.NoError(t, err)
// 			require.NoError(t, s.FilterField(query.NewFilterField(s.ModelStruct.Primary(), query.OpIn, 2, 3)))
//
// 			if assert.NoError(t, s.List()) {
// 				values, ok := s.Value.(*[]*tests.Model)
// 				if assert.True(t, ok, "%T", s.Value) {
// 					for i, sv := range *values {
// 						id := i + 2
//
// 						assert.Equal(t, testModelInstances[id-1].AttrString, sv.AttrString)
// 						assert.Equal(t, testModelInstances[id-1].Int, sv.Int)
// 						assert.Equal(t, id, sv.ID)
// 					}
// 				}
// 			}
// 		})
//
// 		t.Run("#2", func(t *testing.T) {
// 			tm := []*tests.Model{}
//
// 			s, err := query.NewC(c, &tm)
// 			require.NoError(t, err)
//
// 			err = s.List()
// 			require.NoError(t, err)
//
// 			if assert.Len(t, tm, 4) {
// 				for i, s := range tm {
// 					assert.Equal(t, i+1, s.ID)
// 					assert.Equal(t, "some", s.AttrString)
// 				}
//
// 				assert.Equal(t, 2, tm[0].Int)
// 				assert.Equal(t, 5, tm[1].Int)
// 				assert.Equal(t, 1, tm[2].Int)
// 				assert.Equal(t, 2, tm[3].Int)
// 			}
// 		})
// 	})
//
// 	t.Run("OrderBy", func(t *testing.T) {
// 		tm := []*tests.Model{}
//
// 		s, err := query.NewC(c, &tm)
// 		require.NoError(t, err)
//
// 		err = s.Sort("-id")
// 		require.NoError(t, err)
//
// 		err = s.List()
// 		require.NoError(t, err)
//
// 		if assert.Len(t, tm, 4) {
// 			for i, s := range tm {
// 				assert.Equal(t, 4-i, s.ID)
// 				assert.Equal(t, "some", s.AttrString)
// 			}
//
// 			assert.Equal(t, 2, tm[3].Int)
// 			assert.Equal(t, 5, tm[2].Int)
// 			assert.Equal(t, 1, tm[1].Int)
// 			assert.Equal(t, 2, tm[0].Int)
// 		}
// 	})
//
// 	t.Run("Paginate", func(t *testing.T) {
// 		tm := []*tests.Model{}
//
// 		s, err := query.NewC(c, &tm)
// 		require.NoError(t, err)
//
// 		err = s.Limit(2)
// 		require.NoError(t, err)
// 		err = s.Offset(1)
// 		require.NoError(t, err)
//
// 		err = s.List()
// 		require.NoError(t, err)
//
// 		if assert.Len(t, tm, 2) {
// 			for i, s := range tm {
// 				// id = i+2 -> offset = 1, id starts from 1
// 				assert.Equal(t, i+2, s.ID)
// 				assert.Equal(t, "some", s.AttrString)
// 			}
//
// 			assert.Equal(t, 5, tm[0].Int)
// 			assert.Equal(t, 1, tm[1].Int)
// 		}
// 	})
// }
