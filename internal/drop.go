package internal

import (
	"context"
	"fmt"
)

func DropTables(ctx context.Context, conn Connection, tableName, schemaName string) error {
	_, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", schemaName, tableName))
	return err
}
