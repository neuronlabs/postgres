package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testcase struct {
	name   string
	data   string
	result []string
}

// TestSplitWKTStrings tests the splitWKTStrings function.
func TestSplitWKTStrings(t *testing.T) {
	data := []testcase{
		{
			"Simple",
			"POINT(1 2),POINT(4 5),something",
			[]string{"POINT(1 2)", "POINT(4 5)", "something"},
		},

		{
			"WithLineString",
			"LINESTRING(1 2, 2 3),POINT(4 5),something",
			[]string{"LINESTRING(1 2, 2 3)", "POINT(4 5)", "something"},
		},
		{
			"WithPolygon",
			"POLYGON((1 2, 2 3, 3 4, 4 5)),POINT(4 5),something",
			[]string{"POLYGON((1 2, 2 3, 3 4, 4 5))", "POINT(4 5)", "something"},
		},
	}

	for _, v := range data {
		t.Run(v.name, func(t *testing.T) {
			result := SplitFilterStrings(v.data)
			assert.Equal(t, v.result, result)
		})
	}
}
