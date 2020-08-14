package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/tests"
	"github.com/neuronlabs/neuron/mapping"
	"github.com/neuronlabs/neuron/query"
)

func TestBuildUpdateQuery(t *testing.T) {
	c := testingController(t, false, &tests.Model{})
	p := testingRepository(c)

	t.Run("Model", func(t *testing.T) {
		mStruct, err := c.ModelStruct(&tests.Model{})
		require.NoError(t, err)

		s := query.NewScope(mStruct, &tests.Model{AttrString: "Name"})
		q, err := p.buildUpdateModelQuery(s, mapping.FieldSet{mStruct.MustFieldByName("AttrString")})
		require.NoError(t, err)

		assert.Equal(t, "UPDATE public.models SET attr_string = $1 WHERE id = $2", q)
	})

	t.Run("BatchModel", func(t *testing.T) {
		mStruct, err := c.ModelStruct(&tests.Model{})
		require.NoError(t, err)

		s := query.NewScope(mStruct, &tests.Model{ID: 1, AttrString: "Name", Int: 50}, &tests.Model{ID: 2, AttrString: "Surname", Int: 100})

		batch := &internal.DummyBatch{}
		err = p.updateBatchModelsWithFieldSet(s, batch, mapping.FieldSet{mStruct.MustFieldByName("AttrString"), mStruct.MustFieldByName("Int")}, s.Models...)
		require.NoError(t, err)
		assert.Equal(t, 2, batch.Len())
		for i, b := range batch.Queries {
			assert.Equal(t, "UPDATE public.models SET attr_string = $1, int = $2 WHERE id = $3", b.Query)
			switch i {
			case 0:
				assert.ElementsMatch(t, b.Arguments, []interface{}{"Name", 50, 1})
			case 1:
				assert.ElementsMatch(t, b.Arguments, []interface{}{"Surname", 100, 2})
			}
		}
	})
}
