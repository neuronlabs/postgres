package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/tests"
	"github.com/neuronlabs/neuron/mapping"
	"github.com/neuronlabs/neuron/query"
	"github.com/neuronlabs/neuron/query/filter"
)

func TestParseSelect(t *testing.T) {
	c := testingController(t, false, &tests.Model{})
	repo := testingRepository(c)

	mStruct, err := c.ModelStruct(&tests.Model{})
	require.NoError(t, err)

	attrField, ok := mStruct.Attribute("attr_string")
	require.True(t, ok)

	s := query.NewScope(mStruct)
	s.FieldSets = []mapping.FieldSet{mStruct.Fields()}
	s.Filters = filter.Filters{
		filter.New(mStruct.Primary(), filter.OpIn, 3, 4),
		filter.New(attrField, filter.OpEqual, "test"),
	}
	s.Pagination = &query.Pagination{
		Limit:  5,
		Offset: 10,
	}

	sq, err := repo.parseSelectQuery(s)
	require.NoError(t, err)

	assert.Equal(t, "SELECT id, attr_string, string_ptr, int, created_at, updated_at, deleted_at FROM public.models WHERE id IN ($1,$2) AND attr_string = $3 LIMIT $4 OFFSET $5", sq.query)
}
