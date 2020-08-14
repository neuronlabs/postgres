package migrate

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/mapping"
)

var (
	/** Character types */

	// FChar is the 'char' field type.
	FChar = &ParameterDataType{SQLName: "char", DataType: DataType{Name: "char"}}
	// FVarChar is the 'varchar' field type.
	FVarChar = &ParameterDataType{SQLName: "varchar", DataType: DataType{Name: "varchar"}}
	// FText is the 'text' field type.
	FText = &BasicDataType{SQLName: "text", DataType: DataType{Name: "text"}}

	/** Numerics */

	// FSmallInt is the 2 bytes signed 'smallint' - int16.
	FSmallInt = &BasicDataType{SQLName: "smallint", DataType: DataType{Name: "smallint"}}
	// FInteger is the 4 bytes signed 'integer' type - int32.
	FInteger = &BasicDataType{SQLName: "integer", DataType: DataType{Name: "integer"}}
	// FBigInt is the 8 bytes signed 'bigint' type - int64.
	FBigInt = &BasicDataType{SQLName: "bigint", DataType: DataType{Name: "bigint"}}
	// FDecimal is the variable 'decimal' type.
	FDecimal = &ParameterDataType{SQLName: "decimal", DataType: DataType{Name: "decimal"}}
	// FNumeric is the ariable 'numeric' type.
	FNumeric = &ParameterDataType{SQLName: "numeric", DataType: DataType{Name: "numeric"}}
	// FReal is the 4 bytes - 6 decimal digits precision 'real' type.
	FReal = &BasicDataType{SQLName: "real", DataType: DataType{Name: "real"}}
	// FDouble is the 8 bytes - 15 decimal digits precision 'double precision' type.
	FDouble = &BasicDataType{SQLName: "double precision", DataType: DataType{Name: "double"}}
	// FSerial is the 4 bytes - autoincrement integer 'serial' type.
	FSerial = &BasicDataType{SQLName: "serial", DataType: DataType{Name: "serial"}}
	// FBigSerial is the 8 bytes autoincrement big integer - 'bigserial' type.
	FBigSerial = &BasicDataType{SQLName: "bigserial", DataType: DataType{Name: "bigserial"}}

	// FUUID is the uuid
	FUUID = &BasicDataType{SQLName: "uuid", DataType: DataType{Name: "uuid"}}

	/** Binary */

	// FBytea is the 1 or 4 bytes plus the actual binary string data type 'bytea'.
	FBytea = &ParameterDataType{SQLName: "bytea", DataType: DataType{Name: "bytea"}}
	// FBoolean is the 'boolean' pq data type.
	FBoolean = &BasicDataType{SQLName: "boolean", DataType: DataType{Name: "boolean"}}

	/** Date / Time */

	// FDate is the 'date' field kind.
	FDate = &BasicDataType{SQLName: "date", DataType: DataType{Name: "date"}}
	// FTimestamp is the 'timestamp' without time zone data type.
	FTimestamp = &OptionalParameterDataType{SQLNames: []string{"timestamp"}, ParameterIndex: 1, DataType: DataType{Name: "timestamp"}}
	// FTimestampTZ is the 'timestamp with time zone' data type.
	FTimestampTZ = &OptionalParameterDataType{SQLNames: []string{"timestamp", "with time zone"}, ParameterIndex: 1, DataType: DataType{Name: "timestamptz"}}
	// FTime is the 'time' without time zone data type.
	FTime = &OptionalParameterDataType{SQLNames: []string{"time"}, ParameterIndex: 1, DataType: DataType{Name: "time"}}
	// FTimeTZ is the 'time with time zone' data type.
	FTimeTZ = &OptionalParameterDataType{SQLNames: []string{"time", "with time zone"}, ParameterIndex: 1, DataType: DataType{Name: "timetz"}}
)

// dataTypes is the array containing the data types
var (
	dataTypes     = make(map[string]DataTyper)
	defaultKindDT = map[reflect.Kind]DataTyper{
		reflect.Bool:    FBoolean,
		reflect.Int:     FInteger,
		reflect.Int8:    FSmallInt,
		reflect.Int16:   FSmallInt,
		reflect.Int32:   FInteger,
		reflect.Int64:   FBigInt,
		reflect.Uint:    FInteger,
		reflect.Uint8:   FSmallInt,
		reflect.Uint16:  FSmallInt,
		reflect.Uint32:  FInteger,
		reflect.Uint64:  FBigInt,
		reflect.String:  FText,
		reflect.Float32: FReal,
		reflect.Float64: FDouble,
	}
	defaultTypeDT = map[reflect.Type]DataTyper{
		reflect.TypeOf(time.Time{}):  FTimestamp,
		reflect.TypeOf(&time.Time{}): FTimestamp,
	}

	defaultTypes = []DataTyper{
		// Characters
		FChar, FVarChar, FText,
		// Numerics
		FInteger, FSmallInt, FBigInt, FDecimal, FNumeric, FReal, FDouble, FBigSerial, FSerial,
		// Binaries
		FBytea, FBoolean,
		// Times
		FDate, FTimestamp, FTimestampTZ, FTime, FTimeTZ,
		// UUID
		FUUID,
	}
)

type ParameterSetter interface {
	SetParameters(params []string) error
}

// findDataType finds the data type for the provided field
func findDataType(field *mapping.StructField) (DataTyper, error) {
	// For predefined database type
	if field.DatabaseType != "" {
		v, err := parseDataType(field.DatabaseType)
		if err != nil {
			return nil, err
		}
		dt, ok := dataTypes[v[0]]
		if !ok {
			return nil, errors.WrapDetf(mapping.ErrMapping, "model: '%s' field: '%s' database type: '%s' is unknown in the postgres repository", field.ModelStruct(), field, field.DatabaseType)
		}
		dt = dt.Copy()
		if len(v) > 0 {
			if pm, ok := dt.(ParameterSetter); ok {
				if err := pm.SetParameters(v[1:]); err != nil {
					return nil, err
				}
			}
		}
		return dt, nil
	}

	t := field.ReflectField().Type
	if field.Kind() == mapping.KindPrimary {
		// by default for the integer primary keys set the serial or bigserial type
		switch t.Kind() {
		case reflect.Int, reflect.Int16, reflect.Int8, reflect.Int32, reflect.Uint, reflect.Uint32, reflect.Uint8, reflect.Uint16:
			return FSerial.Copy(), nil
		case reflect.Int64, reflect.Uint64:
			return FBigSerial.Copy(), nil
		}
	}

	// Check if the field is UUID.
	if strings.ToLower(t.Name()) == "uuid" && t.Kind() == reflect.Array && t.Len() == 16 {
		return FUUID, nil
	}

	if field.IsCreatedAt() || field.IsDeletedAt() || field.IsUpdatedAt() {
		return FTimestampTZ.Copy(), nil
	}

	var (
		isArray  bool
		arrayLen int
	)

	if t.Kind() == reflect.Slice {
		isArray = true
		t = t.Elem()
		if t.Name() == "byte" {
			// Byte slice maps to bytea.
			return FBytea, nil
		}
	} else if t.Kind() == reflect.Array {
		isArray = true
		arrayLen = t.Len()
		t = t.Elem()
	}

	// at first check type
	dt, ok := defaultTypeDT[t]
	if !ok {
		if t.Kind() == reflect.Ptr {
			dt, ok = defaultTypeDT[t.Elem()]
		}
	}

	if !ok {
		k := t.Kind()
		if k == reflect.Ptr {
			k = t.Elem().Kind()
		}
		dt, ok = defaultKindDT[k]
		if !ok {
			return nil, errors.WrapDetf(errors.ErrInternal, "postgres field type not found. Model: '%s', Field: '%s'", field.ModelStruct().Type().Name(), field.Name())
		}
	}

	dt = dt.Copy()

	if isArray {
		dt = &ArrayDataType{Len: arrayLen, Subtype: dt}
	}
	return dt, nil
}

// ExternalDataTyper is the interface that defines the columns that sets the column outside the table definition.
type ExternalDataTyper interface {
	DataTyper
	// ExternalFunction is the method used to create the column outside of the table definition.
	ExternalFunction(field *mapping.StructField) string
}

// DataTyper is the interface for basic data type methods.
type DataTyper interface {
	// KeyName gets the sql key name.
	KeyName() string
	// GetName creates the column string used within the table definition
	GetName() string
	Copy() DataTyper
}

// DataType is the pq base model defininig the data type.
type DataType struct {
	Name string
}

// KeyName gets the name of the data type.
func (d *DataType) KeyName() string {
	return d.Name
}

// BasicDataType is the InlineDataTyper that sets the basic columns on the base of it's SQLName.
type BasicDataType struct {
	SQLName string
	DataType
}

// GetName creates the inline column definition on the base of it's SQLName.
func (b *BasicDataType) GetName() string {
	return b.SQLName
}

// Copy implements DataTyper.
func (b *BasicDataType) Copy() DataTyper {
	return &(*b)
}

// compile time check of BasicDataType
var _ DataTyper = &BasicDataType{}

// ParameterDataType is the data type that contains the variable parameters.
// i.e. varchar(2) has a single parameter '2'.
type ParameterDataType struct {
	DataType
	SQLName    string
	Validate   func(params []string) error
	Parameters []string
}

// Copy implements DataTyper interface.
func (p *ParameterDataType) Copy() DataTyper {
	cp := &ParameterDataType{
		DataType:   p.DataType,
		SQLName:    p.SQLName,
		Validate:   p.Validate,
		Parameters: make([]string, len(p.Parameters)),
	}
	copy(cp.Parameters, p.Parameters)
	return cp
}

func (p *ParameterDataType) SetParameters(params []string) error {
	if p.Validate != nil {
		err := p.Validate(params)
		if err != nil {
			return err
		}
	}
	p.Parameters = params
	return nil
}

// GetName creates the inline column definition on the base of it's SQLName and Parameters.
func (p *ParameterDataType) GetName() string {
	return p.SQLName + "(" + strings.Join(p.Parameters, ",") + ")"
}

// OptionalParameterDataType is the data type that contains optional parameters.
type OptionalParameterDataType struct {
	DataType
	SQLNames       []string
	ParameterIndex int
	Parameters     []string
}

// Copy implements DataTyper interface.
func (p *OptionalParameterDataType) Copy() DataTyper {
	cp := &OptionalParameterDataType{
		DataType:       p.DataType,
		SQLNames:       make([]string, len(p.SQLNames)),
		ParameterIndex: p.ParameterIndex,
		Parameters:     make([]string, len(p.Parameters)),
	}
	copy(cp.SQLNames, p.SQLNames)
	copy(cp.Parameters, p.Parameters)
	return cp
}

func (p *OptionalParameterDataType) SetParameters(params []string) error {
	param := "(" + strings.Join(params, ",") + ")"
	if p.ParameterIndex == len(p.SQLNames) {
		p.Parameters = append(p.SQLNames, param)
	} else {
		p.Parameters = append(p.SQLNames[:p.ParameterIndex], param)
		p.Parameters = append(p.Parameters, p.SQLNames[p.ParameterIndex+1:]...)
	}
	return nil
}

// GetName creates the inline column definition on the base of it's SQLName and Parameters.
func (p *OptionalParameterDataType) GetName() string {
	if len(p.Parameters) == 0 {
		return strings.Join(p.SQLNames, " ")
	}
	return strings.Join(p.Parameters, " ")
}

type ArrayDataType struct {
	Len     int
	Subtype DataTyper
}

// Copy implements DataTyper.
func (a *ArrayDataType) Copy() DataTyper {
	return &ArrayDataType{
		Len:     a.Len,
		Subtype: a.Subtype.Copy(),
	}
}

func (a *ArrayDataType) KeyName() string {
	return a.Subtype.KeyName() + "[]"
}

func (a *ArrayDataType) GetName() string {
	if a.Len == 0 {
		return a.Subtype.GetName() + "[]"
	}
	return a.Subtype.GetName() + "[" + strconv.Itoa(a.Len) + "]"
}

// RegisterDataType registers the provided datatype assigning it next id.
func RegisterDataType(dt DataTyper) error {
	return registerDataType(dt)
}

func registerDataType(dt DataTyper) error {
	// check it the data type exists
	_, ok := dataTypes[dt.KeyName()]
	if ok {
		return errors.WrapDetf(errors.ErrInternal, "postgres data type: '%s' is already registered", dt.KeyName())
	}

	// set the data type at index
	dataTypes[dt.KeyName()] = dt

	return nil
}

// RegisterRefTypeDT registers default data type for provided reflect.Type.
func RegisterRefTypeDT(t reflect.Type, dt DataTyper, override ...bool) error {
	return registerRefTypeDT(t, dt, override...)
}

func registerRefTypeDT(t reflect.Type, dt DataTyper, override ...bool) error {
	var ov bool
	if len(override) > 0 {
		ov = override[0]
	}
	_, ok := defaultTypeDT[t]
	if ok && !ov {
		return errors.WrapDetf(errors.ErrInternal, "default data typer is already set for given type: '%s'", t.Name())
	}

	defaultTypeDT[t] = dt
	return nil
}

func parseDataType(v string) ([]string, error) {
	i := strings.Index(v, "(")
	if i == -1 {
		return []string{v}, nil
	} else if v[len(v)-1] != ')' {
		return nil, errors.WrapDetf(errors.ErrInternal, "invalid postgres DataType value: '%s'", v)
	}

	return append([]string{v[:i]}, strings.Split(v[i+1:len(v)-1], ",")...), nil
}
