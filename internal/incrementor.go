package internal

import (
	"strconv"

	"github.com/neuronlabs/neuron/query"
)

// Incrementor is the function that returns next query increment value.
// Used to obtain the queries values with the incremented arguments.
func Incrementor(s *query.Scope) int {
	return incrementor(s)
}

// ResetIncrementor resets query's incrementor.
func ResetIncrementor(s *query.Scope) {
	s.StoreSet(IncrementorKey, nil)
}

// StringIncrementor is the function that returns next query increment value in a string form.
// Used to obtain the queries values with the incremented arguments.
func StringIncrementor(s *query.Scope) string {
	return "$" + strconv.Itoa(incrementor(s))
}

func incrementor(s *query.Scope) int {
	inc, _ := s.StoreGet(IncrementorKey)
	if inc == nil {
		inc = 0
	}
	i := inc.(int) + 1

	s.StoreSet(IncrementorKey, i)
	return i
}
