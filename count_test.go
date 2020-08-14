package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/tests"
	"github.com/neuronlabs/neuron/query"
	"github.com/neuronlabs/neuron/query/filter"
)

// TestParseCount tests the count method for the postgres repository
func TestParseCount(t *testing.T) {
	c := testingController(t, false, &tests.Model{})

	p := testingRepository(c)

	mStruct, err := c.ModelStruct(&tests.Model{})
	require.NoError(t, err)

	s := query.NewScope(mStruct)
	s.Filters = filter.Filters{filter.New(mStruct.Primary(), filter.OpIn, 12, 23)}

	q, err := p.parseCountQuery(s)
	require.NoError(t, err)

	assert.Equal(t, "SELECT COUNT(DISTINCT id) FROM public.models WHERE id IN ($1,$2)", q.query)
	assert.ElementsMatch(t, []interface{}{12, 23}, q.values)
}

//
// 	p := &Postgres{}
//
// 	repo, err := c.GetRepository(createModel{})
// 	require.NoError(t, err)
// 	p := repo.(*Postgres)
//
// 	s, err := query.NewC(c, &createModel{})
// 	require.NoError(t, err)
//
// 	err = s.Filter("ID >=", 2)
// 	require.NoError(t, err)
//
// 	err = s.Limit(1000)
// 	require.NoError(t, err)
// 	err = s.Offset(10)
// 	require.NoError(t, err)
//
// 	q, err := p.parseCountQuery(s)
// 	require.NoError(t, err)
//
// 	// No limit and offset would be added to the count query.
// 	assert.Equal(t, "SELECT COUNT(DISTINCT id) FROM public.create_models WHERE create_models.id >= $1", q.query)
// 	assert.Contains(t, q.values, 2)
// }
