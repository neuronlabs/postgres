package filters

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron/query/filter"
)

// TestParseOrGroupFilter tests the OrGroup filter parses.
func TestParseOrGroupFilter(t *testing.T) {
	s := getScope(t)
	orFilter := filter.Or(
		filter.New(s.ModelStruct.Primary(), filter.OpEqual, 12345),
		filter.New(s.ModelStruct.Primary(), filter.OpNotEqual, 54321),
	)
	s.Filters = append(s.Filters, orFilter)

	q, err := ParseFilters(s, internal.DummyQuotedWriteFunc)
	require.NoError(t, err)

	require.Len(t, q, 1)
	assert.Equal(t, fmt.Sprintf("(%s = $1 OR %s <> $2)", s.ModelStruct.Primary().NeuronName(), s.ModelStruct.Primary().NeuronName()), q[0].Query)
}
