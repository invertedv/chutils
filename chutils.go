package chutils

//TODO: add read and write functions to TableDefs -- JSON?
import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
)

type ReadError struct {
	Err string
}

func (e *ReadError) Error() string {
	return e.Err
}

// ChType enum is supported ClickHouse types.
type ChType int

//ClickHouse types supported
//TODO: remove FixedString and look for a >0 value of Length
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
type ChField struct {
	Base   ChType
	Length int
	// e.g. LowCardinality()
	OuterFunc string
}

// Converter method converts an aribtrary value to the ClickHouse type requested.
// Returns the value and a boolean indicating whether this was successful.
func (t ChField) Converter(inValue interface{}) (outValue interface{}, ok bool) {
	var err error
	outValue = inValue
	switch t.Base {
	case String, FixedString:
		switch x := inValue.(type) {
		case float64, float32, int:
			outValue = fmt.Sprintf("%v", x)
		}
		if t.Base == FixedString && len(inValue.(string)) > t.Length {
			return nil, false
		}
	case Float:
		switch x := inValue.(type) {
		case string:
			outValue, err = strconv.ParseFloat(x, t.Length)
			if err != nil {
				return nil, false
			}
		}
	case Int:
		switch x := (inValue).(type) {
		case string:
			outValue, err = strconv.ParseInt(x, 10, t.Length)
			if err != nil {
				return nil, false
			}
			outValue = int(outValue.(int64))
		}
	}
	return outValue, true
}

// LegalValues holds ranges and lists of legal values for a field
type LegalValues struct {
	LowLimit  interface{}
	HighLimit interface{}
	Levels    *map[string]int
}

// Check checks whether chekcVal is a legal value
func (l *LegalValues) Check(checkVal interface{}) (ok bool) {
	ok = true
	switch val := checkVal.(type) {
	case string:
		if l.Levels == nil {
			return
		}
		_, ok = checkVal.(string)
		if !ok {
			return
		}
		// check if this is supposed to be a numeric field.
		for rx, _ := range *l.Levels {
			if val == rx {
				return
			}
		}
	case float64:
		if l.LowLimit == nil || l.HighLimit == nil || l.LowLimit == l.HighLimit {
			return
		}
		// Do range check
		if val >= l.LowLimit.(float64) && val <= l.HighLimit.(float64) {
			return
		}
	case int:
		// If they are the same, that means any value is OK
		if l.LowLimit == nil || l.HighLimit == nil || l.LowLimit == l.HighLimit {
			return
		}
		// Do range check
		if val >= l.LowLimit.(int) && val <= l.HighLimit.(int) {
			return
		}
	}
	ok = false
	return
}

// Update takes a LegalValues struc and updates it with newVal of type varType
// If it's a continuous field, it will update High/Low.
// If it's a descrete field, it will add (if needed) the field to the map and update the count
func (l *LegalValues) Update(newVal string, varType string) bool {
	switch varType {
	case "int":
		v, err := strconv.ParseInt(newVal, 10, 64)
		vint := int(v)
		if err != nil {
			return false
		}
		// first time through
		if l.LowLimit == nil || l.HighLimit == nil {
			l.LowLimit = vint
			l.HighLimit = vint
			return true
		}
		vmax, ok := l.HighLimit.(int)
		if !ok {
			return false
		}
		if vint > vmax {
			l.HighLimit = vint
		}
		vmin, ok := l.LowLimit.(int)
		if !ok {
			return false
		}
		if vint < vmin {
			l.LowLimit = vint
		}
	case "float":
		v, err := strconv.ParseFloat(newVal, 64)
		if err != nil {
			return false
		}
		// first time through
		if l.LowLimit == nil || l.HighLimit == nil {
			l.LowLimit = v
			l.HighLimit = v
			return true
		}
		vmax, ok := l.HighLimit.(float64)
		if !ok {
			return false
		}
		if v > vmax {
			l.HighLimit = v
		}
		vmin, ok := l.LowLimit.(float64)
		if !ok {
			return false
		}
		if v < vmin {
			l.LowLimit = v
		}
	case "string":
		if l.Levels == nil {
			x := make(map[string]int, 0)
			l.Levels = &x
		}
		(*l.Levels)[newVal]++
	}
	return true
}

// A Row is a slice of any values. It is a single row of the table
// This is in the same order of the TableDef FieldDefs slice
type Row []interface{}

// A Table is a slice of Rows
type Table []Row

// Status is the validation status of a particular instance of a ChField
// as judged against its ClickHouse type and acceptable values
type Status int

// Field Validation Status enum type
const (
	// Validation not done
	Pending Status = 0 + iota
	// Value is out of range
	ValueFail
	// Cannot be coerced to correct type
	TypeFail
	// Value calculated from other fields
	Calculated
	// Passed
	Pass
)

//go:generate stringer -type=Status

// TODO: change to *ChField
// FieldDef struct holds the definition of single ClickHouse field
type FieldDef struct {
	Name        string
	ChSpec      ChField
	Description string
	Legal       LegalValues
	Missing     interface{}
	Calculator  func(fs Row) interface{}
}

// Validator method to check the Value of Field is legal
// outValue is the inValue that has the correct type. It is set to its Missing value if the Validation fails.
func (fd FieldDef) Validator(inValue interface{}, r Row, s Status) (outValue interface{}, status Status) {
	status = Pass
	outValue, ok := fd.ChSpec.Converter(inValue)
	if !ok {
		status = TypeFail
		outValue = fd.Missing
		return
	}
	if fd.Legal.Check(outValue) {
		return
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

// TableDef defines a table
type TableDef struct {
	Name   string
	Key    string
	Engine string
	//key is column ordering of table
	FieldDefs map[int]*FieldDef
}

// CreateTable func builds and issues CREATE TABLE ClickHouse statement
func (td TableDef) CreateTable(db *sql.DB) (err error) {
	//db should be database object
	qry := fmt.Sprintf("DROP TABLE IF EXISTS %v", td.Name)
	_, err = db.Exec(qry)
	if err != nil {
		return
	}
	qry = fmt.Sprintf("CREATE TABLE %v (", td.Name)
	for ind := 0; ind < len(td.FieldDefs); ind++ {
		fd := td.FieldDefs[ind]
		ftype := fmt.Sprintf("%s     %v", fd.Name, fd.ChSpec.Base)
		if fd.ChSpec.Base == Int || fd.ChSpec.Base == Float {
			ftype = fmt.Sprintf("%s%d", ftype, fd.ChSpec.Length)
		}
		if fd.ChSpec.Base == FixedString {
			ftype = fmt.Sprintf("%s(%d)", ftype, fd.ChSpec.Length)
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

// Reader is the interface required for Load
type Reader interface {
	Read(nTarget int) (data []Row, err error)
	Reset()
	Close()
}

func (t *Table) InsertRows(l Reader, nRow int) (nInserted int, err error) {
	return 0, nil
}

func Load(td TableDef, t Reader, rowCount int, db *sql.DB) {
	qry := fmt.Sprintf("INSERT INTO %s VALUES \n", td.Name)
	data, _ := t.Read(rowCount)
	for r := 0; r < len(data); r++ {
		qry += "("
		for c := 0; c < len(data[r]); c++ {
			char := ","
			if c == len(data[r])-1 {
				char = ")\n"
			}
			v := data[r][c]
			if td.FieldDefs[c].ChSpec.Base == String {
				v = fmt.Sprintf("'%s'", v)
			}
			qry = fmt.Sprintf("%s %v %s", qry, v, char)
		}
	}
	fmt.Println(qry)
	_, err := db.Exec(qry)
	if err != nil {
		log.Fatalln(err)
	}
}
