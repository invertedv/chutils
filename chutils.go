package chutils

import (
	"fmt"
	"strconv"
)

// Status is the validation status of a particular instance of a ChField
// as judged against its Clickhouse type and acceptable values
type Status int

// Field Validation Status enum type
const (
	Pass Status = 0 + iota
	ValueFail
	TypeFail
	Calculated
	Pending
)

//go:generate stringer -type=Status

// ChType is supported Clickhouse types.
type ChType int

//Clickhouse types supported
const (
	Float ChType = 0 + iota
	Int
	String
	FixedString
)

const (
	MissingString = ""
	MissingFloat  = float64(-1.0)
	MissingInt    = int(-1)
)

// interface with a read/load methods to pull data from A and load to B

//go:generate stringer -type=ChType

// Generates stinger for Status

type ChField struct {
	TypeBase   ChType
	TypeLength int
	// e.g. LowCardinality
	OuterFunc string
}

// Converter method converts an aribtrary value to the Clickhouse type request.
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
		fmt.Println("aiming for Float")
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

type Limits struct {
	Asint     int
	Asfloat64 float64
	Asfloat32 float32
}

// Field struct holds a single field
type FieldDef struct {
	FieldName   string
	ChType      ChField
	Description string
	Levels      []string
	LowLimit    Limits
	HighLimit   Limits
	Calculator  func(fs Row) interface{}
}

// TableDef is a map of field names to FieldDefs
type TableDef struct {
	Name   string
	Key    string
	Engine string
	//key is column ordering of table
	FieldDefs map[int]FieldDef
}

func (td TableDef) CreateTable(db string) {
	//db should be database object
	fmt.Println(db)
}

// A Row is a slice of any values. It is a single row of the table
// This is in the same order of the TableDef FieldDefs slice
type Row []interface{}

// A Table is a slice of Rows
type Table []Row

// Validator method to check the Value of Field is legal
func (fd FieldDef) Validator(inValue interface{}, r Row, s Status) (outValue interface{}, status Status) {
	status = Pass
	outValue, ok := fd.ChType.Converter(inValue)
	fmt.Println("outvalue", outValue)
	if ok {
		//	f.ChType.Converter(f.ChType)
		switch val := outValue.(type) {
		case string:
			// check if this is supposed to be a numeric field.
			for _, rx := range fd.Levels {
				if val == rx {
					return
				}
			}
		case float64:
			fmt.Println("float64")
			if val >= fd.LowLimit.Asfloat64 && val <= fd.HighLimit.Asfloat64 {
				return
			}
		case float32:
			if val >= fd.LowLimit.Asfloat32 && val <= fd.HighLimit.Asfloat32 {
				return
			}
		case int:
			if val >= fd.LowLimit.Asint && val <= fd.HighLimit.Asint {
				return
			}
		}
	} else {
		status = TypeFail
		return
	}
	if fd.Calculator != nil && s != Calculated {
		fmt.Println("calculating")
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
	status = ValueFail
	return
}

/*
should row be a row of pointers?
should Validator return value rather than update it?
*/
