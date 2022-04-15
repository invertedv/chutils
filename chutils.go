package chutils

//TODO: add read and write functions to TableDefs -- JSON?
//TODO: fieldefs map[int]string --> make the key the field name and add a struc feature 'order'
import (
	"database/sql"
	"fmt"
	"os/exec"
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

//ClickHouse base types supported
//TODO: remove FixedString and look for a >0 value of Length
const (
	Unknown     ChType = 0 + iota // Unknown: ClickHouse type is undetermined
	Int                           // Int: Clickhouse type is Integer
	String                        // String: Clickhouse type is String (possibly FixedString)
	FixedString                   // FixedString
	Float                         // Float: Clickhouse type is Float
)

// interface with a read/load methods to pull data from A and load to B

//go:generate stringer -type=ChType

// ChField struc holds a ClickHouse field type
type ChField struct {
	Base      ChType // Base: base type of ClickHouse field
	Length    int    // Length: length of field (0 for String)
	OuterFunc string // OuterFunc: Outer function applied (e.g. LowCardinality())
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

func NewLegalValues() *LegalValues {
	x := make(map[string]int)
	return &LegalValues{Levels: &x}
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

// Status is the validation status of a particular instance of a ChField
// as judged against its ClickHouse type and acceptable values
type Status int

// Field Validation Status enum type
const (
	Pending    Status = 0 + iota // Pending means the validation status is not determined
	ValueFail                    // ValueFail: Value is illegal
	TypeFail                     // TypeFail: value cannot be coerced to correct type
	Calculated                   // Calculated: value is calculated from other fields
	Pass                         // Pass: Value is OK
)

//go:generate stringer -type=Status

// TODO: change to *ChField
// FieldDef struct holds the definition of single ClickHouse field
type FieldDef struct {
	Name        string                   // Name of field
	ChSpec      ChField                  // ChSpec is the Clickhouse specification of field
	Description string                   // Description is an optional description for CREATE TABLE
	Legal       *LegalValues             // Legal are optional bounds/list of legal values
	Missing     interface{}              // Missing is the value used for a field if the value is missing/illegal
	Calculator  func(fs Row) interface{} // Calculator is an optional function to calculate the field if it is
	// missing or illegal
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

// Get returns the FieldDef for field "name", nil if there is not such a field.
func (td *TableDef) Get(name string) (fd *FieldDef) {
	for _, fd := range td.FieldDefs {
		if fd.Name == name {
			return fd
		}
		return nil
	}
	return
}

// Create func builds and issues CREATE TABLE ClickHouse statement
func (td *TableDef) Create(db *sql.DB) (err error) {
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
	// reads rows from the source
	Read(nTarget int) (data []Row, err error)
	Reset()
	Close()
}

func InsertFile(td TableDef, t Reader, rowCount int, db *sql.DB) (err error) {
	qry := fmt.Sprintf("INSERT INTO %s VALUES \n", td.Name)
	data, _ := t.Read(rowCount)
	for r := 0; r < len(data); r++ {
		qry += "("
		for c := 0; c < len(data[r]); c++ {
			//			char := ","
			//			if c == len(data[r])-1 {
			//				char = ")\n"
			//			}
			v := data[r][c]
			if td.FieldDefs[c].ChSpec.Base == String {
				v = fmt.Sprintf("'%s'", v)
			}
			//qry += fmt.Sprintf("%v %s", v, char)
			qry += "a"
		}
	}
	fmt.Println("Loading")
	_, err = db.Exec(qry)
	return
}

type FileFormat int

// TODO examine what format to use for flat files
const (
	CSV FileFormat = 0 + iota
	CSVWithNames
	TabSeparated
)

//go:generate stringer -type=FileFormat

func Fileload(tablename string, filename string, delim string, format FileFormat, options string, host string,
	user string, password string) (err error) {
	cmd := fmt.Sprintf("clickhouse-client --host=%s --user=%s", host, user)
	if password != "" {
		cmd = fmt.Sprintf("%s --password=%s", cmd, password)
	}
	cmd = fmt.Sprintf("%s --format_csv_delimiter='%s'", cmd, delim)
	cmd = fmt.Sprintf("%s --query 'INSERT INTO %s FORMAT %s' < %s", cmd, tablename, format, filename)
	// running clickhouse-client as a command chokes on --query
	c := exec.Command("bash", "-c", cmd)
	err = c.Run()
	return
}
