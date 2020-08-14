package postgres

import (
	"github.com/neuronlabs/neuron/errors"
)

var (
	// ErrPostgres is the major error classification for the postgres repository.
	ErrPostgres = errors.New("postgres")

	// ErrUnmappedError is the error classification for unmapped errors.
	ErrUnmappedError = errors.Wrap(ErrPostgres, "unmapped error")

	// ErrInternal is the internal error in the postgres repository package.
	ErrInternal = errors.Wrap(errors.ErrInternal, "postgres")
)
