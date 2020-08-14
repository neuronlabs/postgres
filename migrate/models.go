package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
	"github.com/neuronlabs/neuron/mapping"
)

// Models inserts model's definitions, indexes and constraints if not exists.
// The models needs to be prepared earlier.
func Models(ctx context.Context, conn internal.Connection, models ...*mapping.ModelStruct) error {
	for _, model := range models {
		if err := migrateModel(ctx, conn, model); err != nil {
			return err
		}
	}
	return nil
}

func migrateModel(ctx context.Context, conn internal.Connection, model *mapping.ModelStruct) error {
	if err := migrateTable(ctx, conn, model); err != nil {
		return err
	}
	if err := migrateConstraints(ctx, conn, model); err != nil {
		return err
	}
	for _, index := range model.DatabaseIndexes() {
		if err := migrateIndex(ctx, conn, model, index); err != nil {
			return err
		}
	}
	return nil
}

func migrateConstraints(ctx context.Context, conn internal.Connection, model *mapping.ModelStruct) error {
	for _, field := range model.Fields() {
		if field.DatabaseSkip() {
			continue
		}
		if field.Kind() == mapping.KindPrimary {
			if err := CPrimaryKey.Execute(ctx, conn, model, field); err != nil {
				return err
			}
			continue
		}

		if field.DatabaseNotNull() {
			if err := CNotNull.Execute(ctx, conn, model, field); err != nil {
				return err
			}
		} else {
			exists, err := CNotNull.DBChecker(ctx, conn, model, field)
			if err != nil {
				return err
			}
			if exists {
				if err := CNotNull.Droper(ctx, conn, model, field); err != nil {
					return err
				}
			}
		}
		if field.DatabaseUnique() {
			if err := CUnique.Execute(ctx, conn, model, field); err != nil {
				return err
			}
		} else {
			exists, err := CUnique.DBChecker(ctx, conn, model, field)
			if err != nil {
				return err
			}
			if exists {
				if err := CUnique.Droper(ctx, conn, model, field); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func migrateTable(ctx context.Context, conn internal.Connection, model *mapping.ModelStruct) error {
	var databaseFields int
	for _, field := range model.Fields() {
		if !field.DatabaseSkip() {
			databaseFields++
		}
	}
	if databaseFields == 0 {
		return nil
	}

	exists, err := existsTable(ctx, conn, model)
	if err != nil {
		return err
	}
	if !exists {
		definitions, err := tableDefinitions(model)
		if err != nil {
			return err
		}
		for _, def := range definitions {
			log.Debugf("Migrate Model Definition Query: \n%s", def)
			if _, err := conn.Exec(ctx, def); err != nil {
				return err
			}
		}
		return nil
	}

	for _, field := range model.Fields() {
		if field.DatabaseSkip() {
			continue
		}
		columnExists, err := existsColumn(ctx, conn, model, field)
		if err != nil {
			return err
		}
		if columnExists {
			continue
		}
		dt, err := findDataType(field)
		if err != nil {
			return err
		}

		if dtt, ok := dt.(ExternalDataTyper); ok {
			if _, err := conn.Exec(ctx, dtt.ExternalFunction(field)); err != nil {
				return err
			}
		} else {
			query := fmt.Sprintf("ALTER TABLE %s.%s ADD %s %s;",
				quoteIdentifier(model.DatabaseSchemaName),
				model.DatabaseName,
				field.DatabaseName,
				dt.GetName(),
			)

			log.Debugf("Updating table: %s column: %s, DB Query: \n%s", model.DatabaseName, field.DatabaseName, query)
			if _, err := conn.Exec(ctx, query); err != nil {
				return err
			}
		}
	}
	return nil
}

// PrepareModels prepares database models
func PrepareModels(models ...*mapping.ModelStruct) error {
	for _, model := range models {
		if err := prepareModel(model); err != nil {
			return err
		}
	}
	return nil
}

func prepareModel(model *mapping.ModelStruct) error {
	if model.DatabaseSchemaName == "" {
		model.DatabaseSchemaName = "public"
	}
	if model.DatabaseName == "" {
		model.DatabaseName = mapping.NamingSnake(model.Collection())
	}

	for _, field := range model.Fields() {
		if field.DatabaseName == "" {
			field.DatabaseName = mapping.NamingSnake(field.Name())
		}
		if _, err := findDataType(field); err != nil {
			return err
		}
		for _, index := range field.DatabaseIndexes() {
			if index.Name == "" {
				index.Name = newIndexName(model, field, index)
			}
		}
	}

	for _, index := range model.DatabaseIndexes() {
		for _, parameter := range index.Parameters {
			switch parameter {
			case BTreeIndex, HashIndex, GiSTIndex, GINIndex:
				index.Type = parameter
			}
		}
		if index.Type == "" {
			index.Type = BTreeIndex
		}
	}
	return nil
}

// tableDefinitions gets model's table definition.
func tableDefinitions(model *mapping.ModelStruct) ([]string, error) {
	sb := &strings.Builder{}

	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
	sb.WriteString(quoteIdentifier(model.DatabaseSchemaName))
	sb.WriteRune('.')
	sb.WriteString(quoteIdentifier(model.DatabaseName))
	sb.WriteString(" (\n")

	var inlineColumns int
	for _, field := range model.Fields() {
		if field.DatabaseSkip() {
			continue
		}
		dt, err := findDataType(field)
		if err != nil {
			return nil, err
		}

		if _, ok := dt.(ExternalDataTyper); !ok {
			inlineColumns++
		}
	}

	var i int
	for _, field := range model.Fields() {
		dt, err := findDataType(field)
		if err != nil {
			return nil, err
		}

		_, ok := dt.(ExternalDataTyper)
		if ok {
			continue
		}
		// write like: 		name type
		sb.WriteString(field.DatabaseName)
		sb.WriteString(" ")
		sb.WriteString(dt.GetName())

		if i < inlineColumns-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
		i++
	}
	sb.WriteString(");")

	var result = []string{sb.String()}

	// write external data types
	for _, field := range model.Fields() {
		dt, err := findDataType(field)
		if err != nil {
			return nil, err
		}

		if dtt, ok := dt.(ExternalDataTyper); ok {
			result = append(result, dtt.ExternalFunction(field))
		}
	}
	return result, nil
}
