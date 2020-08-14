package postgres

import (
	"github.com/jackc/pgconn"

	"github.com/neuronlabs/neuron/query"
	"github.com/neuronlabs/neuron/repository"
)

var pqMapping = map[string]error{
	// Class 02 - No data
	"02":    query.ErrNoResult,
	"P0002": query.ErrNoResult,

	// Class 08 - Connection Exception
	"08": repository.ErrConnection,

	"0B000": query.ErrTxState,

	// Class 21 - Cardinality Violation
	"21": query.ErrInternal,

	// Class 22 Data Exception
	"22": query.ErrFieldValue,

	// Class 23 Integrity Violation errors
	"23":    query.ErrViolationIntegrityConstraint,
	"23000": query.ErrViolationIntegrityConstraint,
	"23001": query.ErrViolationRestrict,
	"23502": query.ErrViolationNotNull,
	"23503": query.ErrViolationForeignKey,
	"23505": query.ErrViolationUnique,
	"23514": query.ErrViolationCheck,

	// Class 25 Invalid Transaction State
	"25": query.ErrTxState,

	// Class 28 Invalid Authorization Specification
	"28000": repository.ErrAuthorization,
	"28P01": repository.ErrAuthorization,

	// Class 2D Invalid Transaction Termination
	"2D000": query.ErrTxState,

	"3D": query.ErrInternal,

	// Class 3F Invalid Schema Name
	"3F":    query.ErrInternal,
	"3F000": query.ErrInternal,

	// Class 40 - Transaction Rollback
	"40": query.ErrTxState,

	// Class 42 - Invalid Syntax
	"42":    query.ErrInternal,
	"42939": repository.ErrReservedName,
	"42804": query.ErrViolationDataType,
	"42703": query.ErrInternal,
	"42883": query.ErrInternal,
	"42P01": query.ErrInternal,
	"42701": query.ErrInternal,
	"42P06": query.ErrInternal,
	"42P07": query.ErrInternal,
	"42501": repository.ErrAuthorization,

	// Class 53 - Insufficient Resources
	"53": repository.ErrRepository,

	// Class 54 - Program Limit Exceeded
	"54": repository.ErrRepository,

	// Class 58 - System Errors
	"58": repository.ErrRepository,

	// Class XX - Internal Error
	"XX": repository.ErrRepository,
}

// Get gets the mapped postgres pq.Error to the neuron error class.
func Get(err error) (error, bool) {
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		return ErrInternal, false
	}

	cl, ok := pqMapping[pgErr.Code]
	if ok {
		return cl, ok
	}
	if len(pgErr.Code) >= 2 {
		cl, ok = pqMapping[pgErr.Code[0:2]]
		return cl, ok
	}
	return ErrInternal, ok
}
