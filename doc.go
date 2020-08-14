// Package postgres implements the github.com/neuronlabs/neuron/repository Factory and Repository interfaces.
// It allows to query PostgreSQL based databases using github.com/jackc/pgx driver.
// The repository implements:
//	- query.FullRepository
// 	- repository.Repository
//	- repository.Migrator
// The repository allows to share single transaction per multiple models - if all are registered within single database.
package postgres
