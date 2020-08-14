package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron/mapping"
)

func migrateIndex(ctx context.Context, conn internal.Connection, model *mapping.ModelStruct, index *mapping.DatabaseIndex) error {
	exists, err := existsIndex(ctx, conn, model, index)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	sb := strings.Builder{}
	sb.WriteString("CREATE ")
	if index.Unique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(indexPrefixer(index.Name))
	sb.WriteString("ON ")
	sb.WriteString(model.DatabaseName)
	if index.Type != BTreeIndex {
		sb.WriteString(" USING ")
		sb.WriteString(index.Type)
	}
	sb.WriteString(" (")
	for i, field := range index.Fields {
		sb.WriteString(field.DatabaseName)
		if i != len(index.Fields)-1 {
			sb.WriteRune(',')
		}
	}
	sb.WriteString(");")
	_, err = conn.Exec(ctx, sb.String())
	return err
}

func newIndexName(model *mapping.ModelStruct, field *mapping.StructField, index *mapping.DatabaseIndex) string {
	if index.Unique {
		return fmt.Sprintf("%s_%s_%s_unique_idx_%d", model.DatabaseSchemaName, model.DatabaseName, field.DatabaseName, len(field.DatabaseIndexes())+1)
	}
	return fmt.Sprintf("%s_%s_%s_idx_%d", model.DatabaseSchemaName, model.DatabaseName, field.DatabaseName, len(field.DatabaseIndexes())+1)
}

func indexPrefixer(indexName string) string {
	return fmt.Sprintf("nrn_auto_%s", indexName)
}
