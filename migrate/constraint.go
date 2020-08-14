package migrate

import (
	"context"
	"fmt"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron/mapping"
)

// field tag constraints
const (
	cNotNull = "notnull"
	cUnique  = "unique"
	cForeign = "foreign"
)

// Constraint defines the postgres constraint type.
type Constraint struct {
	Name      string
	SQLName   func(field *mapping.StructField) (string, error)
	DBChecker func(context.Context, internal.Connection, *mapping.ModelStruct, *mapping.StructField) (bool, error)
	Droper    func(context.Context, internal.Connection, *mapping.ModelStruct, *mapping.StructField) error
}

// Execute checks if given constraints exists and if not executes it using provided connection.
func (c *Constraint) Execute(ctx context.Context, conn internal.Connection, model *mapping.ModelStruct, field *mapping.StructField) error {
	exists, err := c.DBChecker(ctx, conn, model, field)
	if err != nil {
		return err
	}
	if !exists {
		def, err := c.SQLName(field)
		if err != nil {
			return err
		}
		if _, err = conn.Exec(ctx, def); err != nil {
			return err
		}
	}
	return nil
}

func uniqueConstraintName(field *mapping.StructField) string {
	return fmt.Sprintf("unique_%s_%s", field.ModelStruct().DatabaseName, field.DatabaseName)
}

func cNotNullSQLName(field *mapping.StructField) (string, error) {
	return fmt.Sprintf("ALTER TABLE %s.%s ALTER COLUMN %s SET NOT NULL;",
		quoteIdentifier(field.ModelStruct().DatabaseSchemaName), quoteIdentifier(field.ModelStruct().DatabaseName), field.DatabaseName), nil
}

var (
	// CNotNull is the not null constraint
	CNotNull = &Constraint{
		Name:      cNotNull,
		SQLName:   cNotNullSQLName,
		DBChecker: HasNotNullConstraint,
		Droper:    dropNotNull,
	}

	// CUnique is the 'unique' constraint.
	CUnique = &Constraint{
		Name: cUnique,
		SQLName: func(field *mapping.StructField) (string, error) {
			return fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s UNIQUE (%s);",
				quoteIdentifier(field.ModelStruct().DatabaseSchemaName),
				quoteIdentifier(field.ModelStruct().DatabaseName),
				uniqueConstraintName(field),
				field.DatabaseName,
			), nil
		},
		DBChecker: HasUniqueConstraint,
		Droper:    dropUnique,
	}

	// CPrimaryKey is the Primary key constraint.
	CPrimaryKey = &Constraint{
		Name: "primary",
		SQLName: func(field *mapping.StructField) (string, error) {
			return fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY (%s);",
				quoteIdentifier(field.ModelStruct().DatabaseSchemaName),
				quoteIdentifier(field.ModelStruct().DatabaseName),
				field.DatabaseName,
			), nil
		},
		DBChecker: existsPrimaryKey,
	}

	// CForeignKey is the Foreign key constraint.
	CForeignKey = &Constraint{Name: "foreign", SQLName: func(field *mapping.StructField) (string, error) {
		relatedField := field.Relationship().RelatedModelStruct().Primary()
		relatedModel := relatedField.ModelStruct()

		return fmt.Sprintf("ALTER TABLE %s.%s ADD FOREIGN KEY (%s) REFERENCES %s.%s(%s);",
			quoteIdentifier(field.ModelStruct().DatabaseSchemaName),
			quoteIdentifier(field.ModelStruct().DatabaseName),
			field.DatabaseName,
			quoteIdentifier(relatedModel.DatabaseSchemaName),
			quoteIdentifier(relatedModel.DatabaseName),
			relatedField.DatabaseName,
		), nil
	},
		DBChecker: existsForeignKey,
	}
)
