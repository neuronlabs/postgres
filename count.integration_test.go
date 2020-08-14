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
	"github.com/neuronlabs/neuron/mapping"
)

var testModels = []mapping.Model{&tests.Model{}, &tests.SimpleModel{}, &tests.OmitModel{}}

// TestIntegrationCount does integration tests for the Count method.
func TestIntegrationCount(t *testing.T) {
	c := testingController(t, true, testModels...)
	p := testingRepository(c)

	ctx := context.Background()
	mStruct, err := c.ModelStruct(&tests.Model{})
	require.NoError(t, err)

	defer func() {
		_ = internal.DropTables(ctx, p.ConnPool, mStruct.DatabaseName, mStruct.DatabaseSchemaName)
	}()

	db := database.New(c)

	newModel := func() *tests.Model {
		return &tests.Model{
			AttrString: "Something",
			Int:        3,
		}
	}
	models := []mapping.Model{newModel(), newModel()}
	// Insert models.
	err = db.Query(mStruct, models...).Insert()
	require.NoError(t, err)

	assert.Len(t, models, 2)

	countAll, err := db.Query(mStruct).Count()
	require.NoError(t, err)

	assert.Equal(t, int64(2), countAll)
}

// 	c, db := prepareIntegrateRepository(t)
//
// 	defer db.Close()
// 	defer deleteTestModelTable(t, db)
//
// 	s, err := query.NewC(c, &tests.Model{})
// 	require.NoError(t, err)
//
// 	count, err := s.Count()
// 	require.NoError(t, err)
//
// 	// there should be 4 created at 'prepareIntegrateRepository' instances
// 	assert.Equal(t, int64(len(testModelInstances)), count)
//
// 	for i := 0; i < 10; i++ {
// 		tm := &tests.Model{Int: i}
// 		s, err := query.NewC(c, tm)
// 		require.NoError(t, err)
//
// 		err = s.Create()
// 		require.NoError(t, err)
// 	}
//
// 	s, err = query.NewC(c, &tests.Model{})
// 	require.NoError(t, err)
//
// 	count, err = s.Count()
// 	require.NoError(t, err)
//
// 	// there should be 10 models + 4 created at 'prepareIntegrateRepository' instances
// 	assert.Equal(t, int64(10+len(testModelInstances)), count)
// }
