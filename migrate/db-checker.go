package migrate

import (
	"context"
	"fmt"
	"strconv"

	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/mapping"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

// GetVersion gets the numerical version of the postgres server.
func GetVersion(ctx context.Context, conn internal.Connection) (int, error) {
	var version string

	err := conn.QueryRow(ctx, "SHOW server_version_num;").Scan(&version)
	if err != nil {
		log.Debug("Querying server version failed: %v", err)
		return 0, errors.WrapDetf(errors.ErrInternal, "Can't obtain pq server version: %v", err.Error())
	}

	v, err := strconv.Atoi(version)
	if err != nil {
		return 0, errors.WrapDetf(errors.ErrInternal, "Can't get postgres integer version: %v", err.Error())
	}
	return v, nil
}

// existsTable checks if the provided table already exists in the provided database.
func existsTable(ctx context.Context, conn internal.Connection, m *mapping.ModelStruct) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, "SELECT count(*) FROM INFORMATION_SCHEMA.tables WHERE table_name = $1 AND table_type = 'BASE TABLE' AND table_schema = $2", m.DatabaseName, m.DatabaseSchemaName).Scan(&count)
	if err != nil {
		log.Debug("Querying table: '%s' failed: %v", m.DatabaseName, err)
		return false, err
	}
	return count > 0, nil
}

// existsColumn checks if the provided table has given column set in the database.
func existsColumn(ctx context.Context, conn internal.Connection, m *mapping.ModelStruct, field *mapping.StructField) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, "SELECT count(*) FROM INFORMATION_SCHEMA.columns WHERE table_name = $1 AND column_name = $2 AND table_schema = $3", m.DatabaseName, field.DatabaseName, m.DatabaseSchemaName).Scan(&count)
	if err != nil {
		log.Debugf("Querying column for the table: '%s' failed: %v", m.DatabaseName, err)
		return false, err
	}
	return count > 0, nil
}

// existsIndex checks if the following table has provided index.
func existsIndex(ctx context.Context, conn internal.Connection, m *mapping.ModelStruct, i *mapping.DatabaseIndex) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2 AND schemaname = $3", m.DatabaseName, indexPrefixer(i.Name), m.DatabaseSchemaName).Scan(&count)
	if err != nil {
		log.Debugf("Querying indexes for the table: '%s' failed: %v", m.DatabaseName, err)
		return false, err
	}
	return count > 0, nil
}

// existsPrimaryKey checks if the table contains primary key.
func existsPrimaryKey(ctx context.Context, conn internal.Connection, m *mapping.ModelStruct, field *mapping.StructField) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, "SELECT count(*) from information_schema.table_constraints where table_name = $1 and constraint_type = 'PRIMARY KEY'", m.DatabaseName).Scan(&count)
	if err != nil {
		log.Debugf("Querying primary keys for the table: '%s' failed: %v", m.DatabaseName, err)
		return false, err
	}
	return count > 0, nil
}

// existsForeignKey checks if the foreign key exists.
func existsForeignKey(ctx context.Context, conn internal.Connection, m *mapping.ModelStruct, field *mapping.StructField) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, "SELECT count(*) from pq_foreign_keys_view WHERE table_name = $1 and column_name = $2", m.DatabaseName, field.DatabaseName).Scan(&count)
	if err != nil {
		log.Debugf("Querying foreign keys for the table: '%s' failed: %v", m.DatabaseName, err)
		return false, err
	}
	return count > 0, nil
}

// createForeignKeysView creates a sql View for the foreign keys per table.
func createForeignKeysView(ctx context.Context, conn internal.Connection) {
	query := `CREATE OR REPLACE VIEW pq_foreign_keys_view AS
SELECT
    tc.table_name, kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage 
        AS kcu ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage 
        AS ccu ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY';`

	if _, err := conn.Exec(ctx, query); err != nil {
		log.Errorf("Creating Foreign Keys view failed: %v", err)
	}
}

// HasUniqueConstraint checks if the table contains constraint.
func HasUniqueConstraint(ctx context.Context, conn internal.Connection, m *mapping.ModelStruct, field *mapping.StructField) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, "SELECT count(*) from information_schema.table_constraints where table_name = $1 and constraint_type = 'UNIQUE' and constraint_name = $2", m.DatabaseName, uniqueConstraintName(field)).Scan(&count)
	if err != nil {
		log.Debugf("Querying unique constraint for the table: '%s' failed: %v", m.DatabaseName, err)
		return false, err
	}
	return count > 0, nil
}

// HasNotNullConstraint checks if the column has a not null constraint.
func HasNotNullConstraint(ctx context.Context, conn internal.Connection, model *mapping.ModelStruct, field *mapping.StructField) (bool, error) {
	var count int
	err := conn.QueryRow(ctx, "SELECT count(*) from information_schema.columns where table_name = $1 and column_name = $2 and is_nullable = 'NO'", model.DatabaseName, field.DatabaseName).Scan(&count)
	if err != nil {
		log.Debugf("Querying not null constraint for the table: '%s' failed: %v", model.DatabaseName, err)
		return false, err
	}
	return count > 0, nil
}

func dropNotNull(ctx context.Context, conn internal.Connection, model *mapping.ModelStruct, field *mapping.StructField) error {
	_, err := conn.Exec(ctx,
		fmt.Sprintf("ALTER TABLE %s.%s ALTER %s DROP NOT NULL",
			quoteIdentifier(model.DatabaseSchemaName),
			quoteIdentifier(model.DatabaseName),
			field.DatabaseName,
		),
	)
	return err
}

func dropUnique(ctx context.Context, conn internal.Connection, model *mapping.ModelStruct, field *mapping.StructField) error {
	_, err := conn.Exec(ctx,
		fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s",
			quoteIdentifier(model.DatabaseSchemaName),
			quoteIdentifier(model.DatabaseName),
			uniqueConstraintName(field),
		),
	)
	return err
}
