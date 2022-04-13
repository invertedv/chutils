package chutils

//TODO: add read and write functions to TableDefs -- JSON?
import (
	"database/sql"
	"fmt"
	"strconv"
)

// Reader is the interface required for Load
type Reader interface {
	Read(nTarget int) (data []Row, err error)
}

// Status is the validation status of a particular instance of a ChField
// as judged against its ClickHouse type and acceptable values
type Status int

// Field Validation Status enum type
const (
	Pending Status = 0 + iota
	// Value is out of range
	ValueFail
	// Cannot be coerced to correct type
	TypeFail
	// Value calculated from other fields
	Calculated
	// Not yet validated
	Pass
)

//go:generate stringer -type=Status

// ChType enum is supported ClickHouse types.
type ChType int

//ClickHouse types supported
const (
	Unknown ChType = 0 + iota
	Int
	String
	FixedString
	Float
)

// interface with a read/load methods to pull data from A and load to B

//go:generate stringer -type=ChType

// ChField struc holds a ClickHouse field type
// TODO: remove the name "Type" from TypeBase and TypeLength
type ChField struct {
	TypeBase   ChType
	TypeLength int
	// e.g. LowCardinality()
	OuterFunc string
}

// Converter method converts an aribtrary value to the ClickHouse type requested.
// Returns the value and a boolean indicating whether this was successful.
func (t ChField) Converter(inValue interface{}) (outValue interface{}, ok bool) {
	var err error
	outValue = inValue
	switch t.TypeBase {
	case String, FixedString:
		switch x := inValue.(type) {
		case float64, float32, int:
			outValue = fmt.Sprintf("%v", x)
		}
		if t.TypeBase == FixedString && len(inValue.(string)) > t.TypeLength {
			return nil, false
		}
	case Float:
		switch x := inValue.(type) {
		case string:
			outValue, err = strconv.ParseFloat(x, t.TypeLength)
			if err != nil {
				return nil, false
			}
		}
	case Int:
		switch x := (inValue).(type) {
		case string:
			outValue, err = strconv.ParseInt(x, 10, t.TypeLength)
			if err != nil {
				return nil, false
			}
		}
	}
	return outValue, true
}

// Limits holds an upper or lower limit for a field.
// Only one should be populated for the appropriate type.
// Having all three avoids repeated type conversions.
type Limits struct {
	AsInt   int64
	AsFloat float64
}

// TODO change ChType name to something else
// TODO: change to *ChField
// TODO: add a create function
// TODO: change TableDef field name to something else
// ChType -> ChSpec
// TableDef -> TableSpec
// FieldDef struct holds the definition of single ClickHouse field
type FieldDef struct {
	FieldName   string
	ChType      ChField
	Description string
	Levels      map[string]int
	LowLimit    Limits
	HighLimit   Limits
	Missing     interface{}
	Calculator  func(fs Row) interface{}
}

// TableDef is a map of field names to FieldDefs
type TableDef struct {
	TableName string
	Key       string
	Engine    string
	//key is column ordering of table
	FieldDefs map[int]*FieldDef
}

// CreateTable func builds and issues CREATE TABLE ClickHouse statement
func (td TableDef) CreateTable(db *sql.DB) (err error) {
	//db should be database object
	qry := fmt.Sprintf("DROP TABLE IF EXISTS %v", td.TableName)
	_, err = db.Exec(qry)
	if err != nil {
		return
	}
	qry = fmt.Sprintf("CREATE TABLE %v (", td.TableName)
	for ind := 0; ind < len(td.FieldDefs); ind++ {
		fd := td.FieldDefs[ind]
		ftype := fmt.Sprintf("%s     %v", fd.FieldName, fd.ChType.TypeBase)
		if fd.ChType.TypeBase == Int || fd.ChType.TypeBase == Float {
			ftype = fmt.Sprintf("%s%d", ftype, fd.ChType.TypeLength)
		}
		if fd.ChType.TypeBase == FixedString {
			ftype = fmt.Sprintf("%s(%d)", ftype, fd.ChType.TypeLength)
		}
		if fd.Description != "" {
			ftype = fmt.Sprintf("%s     comment '%s'", ftype, fd.Description)
		}
		char := ","
		if ind == len(td.FieldDefs)-1 {
			char = ")"
		}
		ftype = fmt.Sprintf("%s%s\n", ftype, char)
		qry = fmt.Sprintf("%s %s", qry, ftype)
	}
	qry = fmt.Sprintf("%s ENGINE=%s()\nORDER BY (%s)", qry, td.Engine, td.Key)
	_, err = db.Exec(qry)
	fmt.Println(qry)
	return
}

// A Row is a slice of any values. It is a single row of the table
// This is in the same order of the TableDef FieldDefs slice
type Row []interface{}

// A Table is a slice of Rows
type Table []Row

func (t *Table) InsertRows(l Reader, nRow int) (nInserted int, err error) {
	return 0, nil
}

// Validator method to check the Value of Field is legal
// outValue is the inValue that has the correct type. It is set to its Missing value if the Validation fails.
func (fd FieldDef) Validator(inValue interface{}, r Row, s Status) (outValue interface{}, status Status) {
	status = Pass
	outValue, ok := fd.ChType.Converter(inValue)
	if !ok {
		status = TypeFail
		outValue = fd.Missing
		return
	}
	//	f.ChType.Converter(f.ChType)
	//	switch v := reflect.ValueOf(outValue); v.Kind() {
	//	case reflect.Int, reflect.Float64, reflect.Float32:
	//		fmt.Println("Here")
	//	}
	switch val := outValue.(type) {
	case string:
		if fd.Levels == nil {
			return
		}
		// check if this is supposed to be a numeric field.
		for rx, _ := range fd.Levels {
			if val == rx {
				return
			}
		}
	case float64:
		// If they are the same, that means any value is OK
		//		x := reflect.TypeOf(val)
		//		fmt.Println(x)
		if fd.LowLimit.AsFloat == fd.HighLimit.AsFloat {
			return
		}
		// Do range check
		if val >= fd.LowLimit.AsFloat && val <= fd.HighLimit.AsFloat {
			return
		}
	case int64:
		// If they are the same, that means any value is OK
		if fd.LowLimit.AsInt == fd.HighLimit.AsInt {
			return
		}
		// Do range check
		if val >= fd.LowLimit.AsInt && val <= fd.HighLimit.AsInt {
			return
		}
	}
	if fd.Calculator != nil && s != Calculated {
		hold := outValue
		outValue, status = fd.Validator(fd.Calculator(r), r, Calculated)
		// see if estimated value is legal
		if status == Pass {
			status = Calculated
			return
		}
		// if not, put back original value and Fail
		outValue = hold
	}
	outValue = fd.Missing
	status = ValueFail
	return
}

type ReadError struct {
	Err string
}

func (e *ReadError) Error() string {
	return e.Err
}

func ReadSql(data []Row, numRow int) (numLoaded int, err error) {
	return 0, nil
}

func Load(t Reader, db *sql.DB) {

	fmt.Println("Hi")
	x, _ := t.Read(1)
	fmt.Println(x)
}
