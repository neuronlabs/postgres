package migrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTableDefinition tests the table definition functions.
func TestTableDefinition(t *testing.T) {
	t.Run("SimpleModel", func(t *testing.T) {
		// type the some model
		some := &Model{}
		m := tCtrl(t, some)

		mStruct, ok := m.GetModelStruct(some)
		require.True(t, ok)

		def, err := tableDefinitions(mStruct)
		require.NoError(t, err)
		expected := `CREATE TABLE IF NOT EXISTS "public"."models" (
id serial,
attribute text,
snake_cased text,
created_at timestamp with time zone,
updated_at timestamp with time zone,
deleted_at timestamp with time zone
);`
		assert.Equal(t, expected, def[0])
	})

	t.Run("WithPredefinedType", func(t *testing.T) {
		model := &BasicModel{}
		m := tCtrl(t, model)

		mStruct, ok := m.GetModelStruct(model)
		require.True(t, ok)

		def, err := tableDefinitions(mStruct)
		require.NoError(t, err)

		expected := `CREATE TABLE IF NOT EXISTS "public"."basic_models" (
id serial,
string text,
timed timestamp,
ptr_time timestamp,
int integer,
int_16 smallint,
varchar_20 varchar(20),
float_32 real,
int_array integer[3],
int_slice integer[]
);`
		assert.Equal(t, expected, def[0])
	})
}
