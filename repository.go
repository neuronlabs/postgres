package postgres

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/migrate"
	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/mapping"
	"github.com/neuronlabs/neuron/query"
	"github.com/neuronlabs/neuron/repository"
)

// FactoryName defines the name of the factory.
const FactoryName = "postgres"

var (
	// Compile time check if Postgres implements repository.Repository interface.
	_ repository.Repository = &Postgres{}
	// compile time check for the service.Migrator interface.
	_ repository.Migrator = &Postgres{}
)

// Postgres is the neuron repository that allows to query postgres databases.
// It allows to query PostgreSQL based databases using github.com/jackc/pgx driver.
// The repository implements:
//	- query.FullRepository
// 	- repository.Repository
//	- repository.Migrator
// The repository allows to share single transaction per multiple models - if all are registered within single database.
type Postgres struct {
	// ConnPool is the current postgres connection pool.
	ConnPool *pgxpool.Pool
	// Options are the repository options provided on creation.
	Options *repository.Options
	// ConnConfig is the postgres connection config established on the base of the provided options.
	ConnConfig *pgxpool.Config
	// SelectNotNullsOnInsert is an option that requires the repository to select the not null fields on insert.
	SelectNotNullsOnInsert bool

	// id is the unique identification number of given repository instance.
	id uuid.UUID
	// postgresVersion is the numerical version of the postgres server.
	postgresVersion int
	// keywords are the keywords reserved by the current postgres version.
	keywords map[string]migrate.KeyWordType
	// transactions is the storage for the transactions for given postgres repository.
	transactions map[uuid.UUID]pgx.Tx
	// lock is a transaction locker.
	lock sync.RWMutex
}

// New creates new postgres repository with provided options.
func New(options ...repository.Option) *Postgres {
	p := newPostgres()
	for _, option := range options {
		option(p.Options)
	}
	return p
}

// New creates new postgres instance.
func newPostgres() *Postgres {
	return &Postgres{
		id:                     uuid.New(),
		SelectNotNullsOnInsert: true,
		keywords:               map[string]migrate.KeyWordType{},
		transactions:           map[uuid.UUID]pgx.Tx{},
		Options:                &repository.Options{},
	}
}

// ID returns unique repository id.
func (p *Postgres) ID() string {
	return p.id.String()
}

// Close closes given repository connections.
func (p *Postgres) Close(ctx context.Context) (err error) {
	p.ConnPool.Close()
	return nil
}

// Dial implements repository.Postgres interface. Creates a new Connection Pool for given repository.
func (p *Postgres) Dial(ctx context.Context) (err error) {
	// Get the pool config.
	p.ConnConfig, err = internal.PoolConfig(p.Options)
	if err != nil {
		return err
	}

	// Establish connection with provided config.
	if err = p.establishConnection(ctx); err != nil {
		return err
	}

	// Read postgres version.
	p.postgresVersion, err = migrate.GetVersion(ctx, p.ConnPool)
	if err != nil {
		return err
	}

	// Get and store keywords for current postgres version.
	p.keywords, err = migrate.GetKeyWords(ctx, p.ConnPool, p.postgresVersion)
	if err != nil {
		log.Errorf("Getting keywords for the postgres version: '%d' failed: %v", p.postgresVersion, err)
		return err
	}
	return nil
}

// FactoryName returns the name of the factory for this Postgres.
// Implements repository.Repository interface.
func (p *Postgres) FactoryName() string {
	return FactoryName
}

// Models implements repository.Migrator interface.
// The method creates models tables if not exists and updates the columns per given model fields.
func (p *Postgres) MigrateModels(ctx context.Context, models ...*mapping.ModelStruct) error {
	if p.ConnPool == nil {
		return errors.Wrapf(repository.ErrConnection, "no connection established")
	}
	if err := migrate.Models(ctx, p.ConnPool, models...); err != nil {
		return err
	}
	return nil
}

// HealthCheck implements repository.Repository interface.
// It creates basic queries that checks if the connection is alive and returns given health response.
// The health response contains also notes with postgres version.
func (p *Postgres) HealthCheck(ctx context.Context) (*repository.HealthResponse, error) {
	if p.ConnPool == nil {
		// if no pool is defined than no Dial method was done.
		return nil, errors.Wrapf(repository.ErrConnection, "no connection established")
	}
	var temp string
	if err := p.ConnPool.QueryRow(ctx, "SELECT 1").Scan(&temp); err != nil {
		return &repository.HealthResponse{
			Status: repository.StatusFail,
			Output: err.Error(),
		}, nil
	}

	if err := p.ConnPool.QueryRow(ctx, "SELECT VERSION()").Scan(&temp); err != nil {
		return &repository.HealthResponse{
			Status: repository.StatusFail,
			Output: err.Error(),
		}, nil
	}
	// the repository is healthy.
	return &repository.HealthResponse{
		Status: repository.StatusPass,
		Notes:  []string{temp},
	}, nil
}

// RegisterModels implements repository.Repository interface.
func (p *Postgres) RegisterModels(models ...*mapping.ModelStruct) error {
	return migrate.PrepareModels(models...)
}

/**

Private Methods

*/

// establishConnection Creates new database connection based on te provided DBConfig.
func (p *Postgres) establishConnection(ctx context.Context) (err error) {
	p.ConnPool, err = pgxpool.ConnectConfig(ctx, p.ConnConfig)
	if err != nil {
		return errors.WrapDetf(repository.ErrConnection, "cannot open database connection: %s", err.Error())
	}
	conn, err := p.ConnPool.Acquire(ctx)
	if err != nil {
		return errors.WrapDetf(repository.ErrConnection, "cannot open database connection: %s", err.Error())
	}
	if err = conn.Conn().Ping(ctx); err != nil {
		return errors.WrapDet(repository.ErrConnection, "cannot establish database connection for pq repository")
	}
	return nil
}

func (p *Postgres) neuronError(err error) error {
	mapped, ok := Get(err)
	if ok {
		return mapped
	}
	return ErrUnmappedError
}

func (p *Postgres) connection(s *query.Scope) internal.Connection {
	if tx := s.Transaction; tx != nil {
		return p.getTransaction(tx.ID)
	}
	return p.ConnPool
}

func (p *Postgres) getTransaction(id uuid.UUID) pgx.Tx {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.transactions[id]
}

func (p *Postgres) clearTransaction(id uuid.UUID) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.transactions, id)
}

func (p *Postgres) setTransaction(id uuid.UUID, tx pgx.Tx) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.transactions[id] = tx
}

func (p *Postgres) checkTransaction(id uuid.UUID) (pgx.Tx, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	tx, ok := p.transactions[id]
	return tx, ok
}
