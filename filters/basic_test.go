package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron/query/filter"
)

// TestBasicSQLizer test the basic sqlizer functions
func TestBasicSQLizer(t *testing.T) {
	t.Run("Single", func(t *testing.T) {
		s := getScope(t)
		f := filter.New(s.ModelStruct.Primary(), filter.OpEqual, 12345)

		queries, err := BasicSQLizer(s, internal.DummyQuotedWriteFunc, f)
		require.NoError(t, err)
		require.Len(t, queries, 1)

		assert.Equal(t, "id = $1", queries[0].Query)
		if assert.Len(t, queries[0].Values, 1) {
			assert.Equal(t, 12345, queries[0].Values[0])
		}
	})

	t.Run("Multiple", func(t *testing.T) {
		s := getScope(t)
		f := filter.New(s.ModelStruct.Primary(), filter.OpEqual, 12345, 6789)

		queries, err := BasicSQLizer(s, internal.DummyQuotedWriteFunc, f)
		require.NoError(t, err)

		require.Len(t, queries, 2)

		assert.Equal(t, "id = $1", queries[0].Query)
		if assert.Len(t, queries[0].Values, 1) {
			assert.Equal(t, 12345, queries[0].Values[0])
		}

		assert.Equal(t, "id = $2", queries[1].Query)
		if assert.Len(t, queries[1].Values, 1) {
			assert.Equal(t, 6789, queries[1].Values[0])
		}
	})
}

// TestInSQLizer tests the INSQLizer function
func TestInSQLizer(t *testing.T) {
	t.Run("Single", func(t *testing.T) {
		s := getScope(t)
		f := filter.New(s.ModelStruct.Primary(), filter.OpIn, 12345)

		queries, err := InSQLizer(s, internal.DummyQuotedWriteFunc, f)
		require.NoError(t, err)

		require.Len(t, queries, 1)

		assert.Equal(t, "id IN ($1)", queries[0].Query)
		if assert.Len(t, queries[0].Values, 1) {
			assert.Equal(t, 12345, queries[0].Values[0])
		}
	})

	t.Run("Multiple", func(t *testing.T) {
		s := getScope(t)
		f := filter.New(s.ModelStruct.Primary(), filter.OpIn, 12345, 6789)

		queries, err := InSQLizer(s, internal.DummyQuotedWriteFunc, f)
		require.NoError(t, err)

		require.Len(t, queries, 1)

		assert.Equal(t, "id IN ($1,$2)", queries[0].Query)
		if assert.Len(t, queries[0].Values, 2) {
			assert.Equal(t, 12345, queries[0].Values[0])
			assert.Equal(t, 6789, queries[0].Values[1])
		}
	})
}

// TestStringOperatorsSQLizer test the string value sqlizers
func TestStringOperatorsSQLizer(t *testing.T) {
	t.Run("Contains", func(t *testing.T) {
		s := getScope(t)
		f := filter.New(s.ModelStruct.Primary(), filter.OpContains, "name")

		queries, err := StringOperatorsSQLizer(s, internal.DummyQuotedWriteFunc, f)
		require.NoError(t, err)

		require.Len(t, queries, 1)

		assert.Equal(t, "id LIKE $1", queries[0].Query)
		if assert.Len(t, queries[0].Values, 1) {
			assert.Equal(t, "%name%", queries[0].Values[0])
		}
	})

	t.Run("StartsWith", func(t *testing.T) {
		s := getScope(t)
		f := filter.New(s.ModelStruct.Primary(), filter.OpStartsWith, "name")

		queries, err := StringOperatorsSQLizer(s, internal.DummyQuotedWriteFunc, f)
		require.NoError(t, err)

		require.Len(t, queries, 1)

		assert.Equal(t, "id LIKE $1", queries[0].Query)
		if assert.Len(t, queries[0].Values, 1) {
			assert.Equal(t, "name%", queries[0].Values[0])
		}
	})

	t.Run("EndsWith", func(t *testing.T) {
		s := getScope(t)
		f := filter.New(s.ModelStruct.Primary(), filter.OpEndsWith, "name")

		queries, err := StringOperatorsSQLizer(s, internal.DummyQuotedWriteFunc, f)
		require.NoError(t, err)

		require.Len(t, queries, 1)

		assert.Equal(t, "id LIKE $1", queries[0].Query)
		if assert.Len(t, queries[0].Values, 1) {
			assert.Equal(t, "%name", queries[0].Values[0])
		}
	})

	t.Run("Multiple", func(t *testing.T) {
		s := getScope(t)
		f := filter.New(s.ModelStruct.Primary(), filter.OpContains, "name", "surname")

		queries, err := StringOperatorsSQLizer(s, internal.DummyQuotedWriteFunc, f)
		require.NoError(t, err)

		require.Len(t, queries, 2)

		assert.Equal(t, "id LIKE $1", queries[0].Query)
		if assert.Len(t, queries[0].Values, 1) {
			assert.Equal(t, "%name%", queries[0].Values[0])
		}

		assert.Equal(t, "id LIKE $2", queries[1].Query)
		if assert.Len(t, queries[1].Values, 1) {
			assert.Equal(t, "%surname%", queries[1].Values[0])
		}
	})
}
