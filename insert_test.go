package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/tests"
	"github.com/neuronlabs/neuron/mapping"
	"github.com/neuronlabs/neuron/query"
)

func TestParseInsertQuery(t *testing.T) {
	c := testingController(t, false, &tests.Model{})

	repo := testingRepository(c)

	m, err := c.ModelStruct(&tests.Model{})
	require.NoError(t, err)

	model := &tests.Model{
		AttrString: "some",
		Int:        1,
		CreatedAt:  time.Now(),
	}

	s := query.NewScope(m, model)
	// get rid of the primary field.
	s.FieldSets = []mapping.FieldSet{m.Fields()[1:]}
	q, err := repo.parseInsertWithCommonFieldSet(s)
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO public.models (attr_string,string_ptr,int,created_at,updated_at,deleted_at) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id", q.query)
	if assert.Len(t, q.values, 6) {
		assert.Equal(t, model.AttrString, q.values[0])
		assert.Equal(t, model.StringPtr, q.values[1])
		assert.Equal(t, model.Int, q.values[2])
		assert.Equal(t, model.CreatedAt, q.values[3])
		assert.Equal(t, model.UpdatedAt, q.values[4])
		assert.Equal(t, model.DeletedAt, q.values[5])
	}
}

func TestParseWithDefault(t *testing.T) {
	c := testingController(t, false, &tests.Model{})
	p := testingRepository(c)

	m, err := c.ModelStruct(&tests.Model{})
	require.NoError(t, err)

	model := &tests.Model{
		AttrString: "some",
		Int:        1,
		CreatedAt:  time.Now(),
	}

	p.SelectNotNullsOnInsert = false
	s := query.NewScope(m, model)
	s.FieldSets = append(s.FieldSets, mapping.FieldSet{})

	q, err := p.parseInsertWithCommonFieldSet(s)
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO public.models VALUES (DEFAULT) RETURNING id", q.query)
	assert.Len(t, q.values, 0)
}

func TestParseBulkInsert(t *testing.T) {
	c := testingController(t, false, &tests.Model{})

	repo := testingRepository(c)

	m, err := c.ModelStruct(&tests.Model{})
	require.NoError(t, err)

	batch := &internal.DummyBatch{}

	model := &tests.Model{
		ID:         1,
		AttrString: "Some",
		Int:        3,
	}
	model2 := &tests.Model{
		AttrString: "Model",
		CreatedAt:  time.Now(),
	}
	model3 := &tests.Model{
		AttrString: "Model3",
		CreatedAt:  time.Now(),
		Int:        3,
	}
	firstFieldset := mapping.FieldSet{m.MustFieldByName("ID"), m.MustFieldByName("AttrString"), m.MustFieldByName("Int"), m.MustFieldByName("CreatedAt")}
	secondFieldset := mapping.FieldSet{m.MustFieldByName("AttrString"), m.MustFieldByName("CreatedAt")}
	s := query.NewScope(m, model, model2, model3)
	s.FieldSets = []mapping.FieldSet{firstFieldset, secondFieldset, secondFieldset}
	queryIndices, err := repo.parseInsertBulkFieldsetQuery(s, batch)
	require.NoError(t, err)

	if assert.Len(t, queryIndices, 2) {
		assert.Len(t, queryIndices[0], 0)
		if assert.Len(t, queryIndices[1], 2) {
			assert.Equal(t, queryIndices[1][0], 1)
			assert.Equal(t, queryIndices[1][1], 2)
		}
	}

	firstQuery := batch.Queries[0]
	assert.Equal(t, "INSERT INTO public.models (id,attr_string,int,created_at) VALUES ($1,$2,$3,$4)", firstQuery.Query)
	if assert.Len(t, firstQuery.Arguments, 4) {
		assert.Equal(t, model.ID, firstQuery.Arguments[0])
		assert.Equal(t, model.AttrString, firstQuery.Arguments[1])
		assert.Equal(t, model.Int, firstQuery.Arguments[2])
		assert.Equal(t, time.Time{}, firstQuery.Arguments[3])
	}

	secondQuery := batch.Queries[1]
	assert.Equal(t, "INSERT INTO public.models (attr_string,int,created_at) VALUES ($1,$2,$3),($4,$5,$6) RETURNING id", secondQuery.Query)
	if assert.Len(t, secondQuery.Arguments, 6) {
		assert.Equal(t, model2.AttrString, secondQuery.Arguments[0])
		// This field was auto selected - it must be a zero value.
		assert.Equal(t, 0, secondQuery.Arguments[1])
		assert.Equal(t, model2.CreatedAt, secondQuery.Arguments[2])
		assert.Equal(t, model3.AttrString, secondQuery.Arguments[3])
		// This field was auto selected - it must be a zero value.
		assert.Equal(t, 0, secondQuery.Arguments[4])
		assert.Equal(t, model3.CreatedAt, secondQuery.Arguments[5])
	}
}
