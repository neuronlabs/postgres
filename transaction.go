package postgres

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/query"
	"github.com/neuronlabs/neuron/repository"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

// compile time check for the repository.Transactioner interface.
var _ repository.Transactioner = &Postgres{}

// Begin starts a transaction for the given scope.
// Implements Begin method of the query.Transactioner interface.
func (p *Postgres) Begin(ctx context.Context, tx *query.Transaction) error {
	if _, ok := p.checkTransaction(tx.ID); ok {
		return nil
	}
	var isolation pgx.TxIsoLevel
	txOpts := pgx.TxOptions{IsoLevel: isolation}
	if tx.Options != nil {
		switch tx.Options.Isolation {
		case query.LevelDefault:
		case query.LevelSerializable:
			isolation = pgx.Serializable
		case query.LevelReadCommitted:
			isolation = pgx.ReadCommitted
		case query.LevelReadUncommitted:
			isolation = pgx.ReadUncommitted
		case query.LevelRepeatableRead, query.LevelSnapshot:
			isolation = pgx.RepeatableRead
		default:
			return errors.WrapDetf(query.ErrTxState, "unsupported isolation level: %s", tx.Options.Isolation.String())
		}
		txOpts.IsoLevel = isolation
		if tx.Options.ReadOnly {
			txOpts.AccessMode = pgx.ReadOnly
		}
	}

	pgxTx, err := p.ConnPool.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}
	if log.Level().IsAllowed(log.LevelDebug3) {
		log.Debug3f("[POSTGRES:%s][TX:%s] BEGIN;", p.id, tx.ID)
	}
	p.setTransaction(tx.ID, pgxTx)
	return nil
}

// Commit commits the scope's transaction
// Implements Commit method from the query.Commiter interface
func (p *Postgres) Commit(ctx context.Context, tx *query.Transaction) error {
	if tx == nil {
		return errors.WrapDet(query.ErrTxInvalid, "scope's transaction is nil")
	}

	pgxTx, ok := p.checkTransaction(tx.ID)
	if !ok {
		log.Errorf("Transaction: '%s' no mapped SQL transaction found", tx.ID)
		return errors.WrapDet(query.ErrTxInvalid, "no mapped sql transaction found for the scope")
	}
	defer p.clearTransaction(tx.ID)
	for {
		err := pgxTx.Commit(ctx)
		if err == nil {
			break
		}
		if pgconn.SafeToRetry(err) {
			continue
		}
		return errors.WrapDetf(p.neuronError(err), "commit transaction: %s failed: %v", tx.ID, err)
	}
	return nil
}

// Rollback rolls back the transaction for given scope
func (p *Postgres) Rollback(ctx context.Context, tx *query.Transaction) error {
	if tx == nil {
		return errors.WrapDet(query.ErrTxInvalid, "scope's transaction is nil")
	}
	pgxTx, ok := p.checkTransaction(tx.ID)
	if !ok {
		log.Errorf("Transaction: '%s' no mapped SQL transaction found", tx.ID)
		return errors.WrapDet(query.ErrTxInvalid, "no mapped sql transaction found for the scope")
	}
	defer p.clearTransaction(tx.ID)

	for {
		err := pgxTx.Rollback(ctx)
		if err == nil {
			break
		}
		if pgconn.SafeToRetry(err) {
			continue
		}
		return errors.WrapDetf(p.neuronError(err), "rollback transaction: %s failed: %v", tx.ID, err)
	}
	return nil
}
