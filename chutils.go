package chutils

//TODO: add read and write functions to TableDefs -- JSON?
//TODO: add multiple date formats

import (
	"database/sql"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// Input is the interface for reading source data (csv, flat file, query)
type Input interface {
	Read(nTarget int, validate bool) (data []Row, err error) // Read reads rows from the source
	Reset()                                                  // Resets to beginning of table
	Close()                                                  // Closes file (for file-based sources)
}

type InputError struct {
	Err string
}

func (e *InputError) Error() string {
	return e.Err
}

// ChType enum is supported ClickHouse types.
type ChType int

//TODO: remove FixedString and look for a >0 value of Length
//ClickHouse base types supported
const (
	Unknown     ChType = 0 + iota // Unknown: ClickHouse type is undetermined
	Int                           // Int: ClickHouse type is Integer
	String                        // String: ClickHouse type is String (possibly FixedString)
	FixedString                   // FixedString
	Float                         // Float: ClickHouse type is Float
	Date                          // Date: ClickHouse type is Date
)

// interface with a read/load methods to pull data from A and load to B

//go:generate stringer -type=ChType

// ChField struc holds a ClickHouse field type
type ChField struct {
	Base       ChType // Base: base type of ClickHouse field
	Length     int    // Length: length of field (0 for String)
	OuterFunc  string // OuterFunc: Outer function applied (e.g. LowCardinality())
	DateFormat string // Format for input dates
}

// Converter method converts an arbitrary value to the ClickHouse type requested.
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
		case int:
			fmt.Println("FILL ME IN")
		}
	case Int:
		switch x := (inValue).(type) {
		case string:
			outValue, err = strconv.ParseInt(x, 10, t.Length)
			if err != nil {
				return nil, false
			}
			outValue = int(outValue.(int64))
		case float64, float32:
			fmt.Println("FILL ME IN")
		}
	case Date:
		switch x := (inValue).(type) {
		case string:
			outValue, err = time.Parse(t.DateFormat, x)
			if err != nil {
				return nil, false
			}
		case float64, float32:
			fmt.Println("FILL ME IN")
		}
	}
	return outValue, true
}

// LegalValues holds bounds and lists of legal values for a field
type LegalValues struct {
	LowLimit  interface{} // Minimum legal value for Int, Float
	HighLimit interface{} // Maximum legal value for Int, Float
	FirstDate *time.Time
	LastDate  *time.Time
	Levels    *map[string]int // Legal values for String, FixedString
}

// NewLegalValues creates a new LegalValues type
func NewLegalValues() *LegalValues {
	x := make(map[string]int)
	return &LegalValues{Levels: &x}
}

// Check checks whether checkVal is a legal value
func (l *LegalValues) Check(checkVal interface{}) (ok bool) {
	ok = true
	switch val := checkVal.(type) {
	case string:
		if l.Levels == nil {
			return
		}
		// check if this is supposed to be a numeric field.
		for rx := range *l.Levels {
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
	// TODO: fill this in
	case time.Time:
		if l.FirstDate == nil || l.LastDate == nil {
			return
		}
		if val.Sub(*l.FirstDate).Hours() >= 0 && (*l).LastDate.Sub(val) >= 0 {
			return
		}
	}
	ok = false
	return
}

// Update takes a LegalValues struc and updates it with newVal
// If it's a int/float field, it will update High/Low.
// If it's a descrete field, it will add (if needed) the field to the map and update the count
func (l *LegalValues) Update(newVal string, target ChType) (res ChType) {
	// if target != Unknown, this will force the type indicated by target

	res = target

	// Figure out what this newVal might be
	if res == Unknown {
		// order of assigning: Int, Float, String
		// LowLimit, HighLimit are float64. Converted to int later (if need be)
		res = String
		// int ?
		if _, err := strconv.ParseInt(newVal, 10, 64); err == nil {
			res = Int
		}
		// float ?
		_, err := strconv.ParseFloat(newVal, 64)
		if err == nil && res == String {
			res = Float
		}
		// date ?
		if _, err := time.Parse("2006-01-02", newVal); err == nil {
			res = Date
		}
	}

	switch res {
	case Int, Float:
		v, err := strconv.ParseFloat(newVal, 64)
		if err != nil {
			return
		}
		if l.LowLimit == nil || l.HighLimit == nil {
			l.LowLimit = v
			l.HighLimit = v
			return
		}
		if v > l.HighLimit.(float64) {
			l.HighLimit = v
		}
		if v < l.LowLimit.(float64) {
			l.LowLimit = v
		}
	case String:
		if l.Levels == nil {
			res = String
			x := make(map[string]int, 0)
			l.Levels = &x
		}
		(*l.Levels)[newVal]++
	case Date:
		vx, err := time.Parse("2006-01-02", newVal)
		if err != nil {
			return
		}
		if l.FirstDate == nil || l.LastDate == nil {
			l.FirstDate = &vx
			l.LastDate = &vx
			return
		}
		if (*l).FirstDate.Sub(vx) > 0 {
			l.FirstDate = &vx
		}
		if (*l).LastDate.Sub(vx) < 0 {
			l.LastDate = &vx
		}
		return
	}
	return
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

// TODO: change to *ChField?

// FieldDef struct holds the definition of single ClickHouse field
type FieldDef struct {
	Name        string                   // Name of field
	ChSpec      ChField                  // ChSpec is the Clickhouse specification of field
	Description string                   // Description is an optional description for CREATE TABLE
	Legal       *LegalValues             // Legal are optional bounds/list of legal values
	Missing     interface{}              // Missing is the value used for a field if the value is missing/illegal
	Calculator  func(fs Row) interface{} // Calculator is an optional function to calculate the field
}

// Validator method to check the Value of Field is legal
// outValue is the inValue that has the correct type. It is set to its Missing value if the Validation fails.
func (fd *FieldDef) Validator(inValue interface{}, r Row, s Status) (outValue interface{}, status Status) {
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
func (td *TableDef) Get(name string) *FieldDef {
	for _, fdx := range td.FieldDefs {
		if fdx.Name == name {
			return fdx
		}
	}
	return nil
}

// TODO: change this to stream
// Impute looks at the data and builds the FieldDefs
func (td *TableDef) Impute(rdr Input, rowsToExamine int, tol float64, fuzz int) (err error) {
	// countType keeps track of the field values as the csv is read
	type countType struct {
		floats int
		ints   int
		dates  int
		legal  *LegalValues
	}
	counts := make([]*countType, 0)
	for ind := 0; ind < len(td.FieldDefs); ind++ {
		ct := &countType{legal: NewLegalValues()}
		counts = append(counts, ct)
	}
	numFields := len(td.FieldDefs)
	// now look at RowsToExamine rows to see what types we have
	data, err := rdr.Read(rowsToExamine, false)
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		return
	}

	rowCount := 0
	for rowCount = 0; rowCount < len(data); rowCount++ {
		for ind := 0; ind < len(data[rowCount]); ind++ {
			fval := fmt.Sprintf("%s", data[rowCount][ind])
			switch counts[ind].legal.Update(fval, td.FieldDefs[ind].ChSpec.Base) {
			case Int:
				counts[ind].ints++
			case Float:
				counts[ind].floats++
			case Date:
				counts[ind].dates++
			}
		}
	}
	// threshold to determine which type a field is (100*tol % agreement)
	thresh := int(math.Max(1.0, tol*float64(rowCount)))
	for ind := 0; ind < numFields; ind++ {
		fd := td.FieldDefs[ind]
		// only impute type if user has not specified it
		if fd.ChSpec.Base == Unknown {
			switch {
			case counts[ind].dates >= thresh:
				fd.ChSpec.Base, fd.ChSpec.Length, fd.ChSpec.DateFormat = Date, 0, "2006-01-02"
				fd.Legal.Levels, fd.Legal.HighLimit, fd.Legal.LowLimit = nil, nil, nil
				fd.Legal.FirstDate, fd.Legal.LastDate = counts[ind].legal.FirstDate, counts[ind].legal.LastDate
				fd.Missing = time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC)
			case counts[ind].ints >= thresh:
				fd.ChSpec.Base, fd.ChSpec.Length = Int, 64
				fd.Missing = math.MaxInt32
			case (counts[ind].ints + counts[ind].floats) >= thresh:
				// Some values may convert to int in the file -- these could also be floats
				td.FieldDefs[ind].ChSpec.Base, td.FieldDefs[ind].ChSpec.Length = Float, 64
				td.FieldDefs[ind].Missing = math.MaxFloat32
			default:
				fd.ChSpec.Base = String
				fd.Missing = "Missing"
			}
		}
		switch fd.ChSpec.Base {
		case Int:
			// Convert LowLimit, HighLimit to int
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit =
				nil, int(counts[ind].legal.LowLimit.(float64)), int(counts[ind].legal.HighLimit.(float64))
		case Float:
			// Some values may convert to int in the file -- these could also be floats
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit =
				nil, counts[ind].legal.LowLimit, counts[ind].legal.HighLimit
		default:
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit = counts[ind].legal.Levels, nil, nil
		}

		if fuzz > 0 && (td.FieldDefs[ind].ChSpec.Base == String ||
			td.FieldDefs[ind].ChSpec.Base == FixedString) {
			// drop any with counts below fuzz
			for k, f := range *td.FieldDefs[ind].Legal.Levels {
				// TODO: change fuzz to float??
				if f <= fuzz {
					delete(*td.FieldDefs[ind].Legal.Levels, k)
				}
			}
		}
	}
	rdr.Reset()
	return nil
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
		switch fd.ChSpec.Base {
		case Int, Float:
			ftype = fmt.Sprintf("%s%d", ftype, fd.ChSpec.Length)
		case FixedString:
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
	fmt.Println(qry)
	_, err = db.Exec(qry)
	return
}

// InsertData creates and issues a ClickHouse INSERT Statement
// Should only be used with small amounts of data
func (td *TableDef) InsertData(t Input, rowCount int, db *sql.DB) (err error) {
	qry := fmt.Sprintf("INSERT INTO %s VALUES \n", td.Name)
	data, _ := t.Read(rowCount, true)
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
			qry += fmt.Sprintf("%v %s", v, char)
		}
	}
	_, err = db.Exec(qry)
	return
}

// FileFormat enum has supported file types for bulk insert
type FileFormat int

// TODO examine what format to use for flat files
const (
	CSV FileFormat = 0 + iota
	CSVWithNames
	TabSeparated
)

//go:generate stringer -type=FileFormat

// InsertFile uses clickhouse-client to bulk insert a file
func InsertFile(tablename string, filename string, delim rune, format FileFormat, options string, host string,
	user string, password string) (err error) {
	cmd := fmt.Sprintf("clickhouse-client --host=%s --user=%s", host, user)
	if password != "" {
		cmd = fmt.Sprintf("%s --password=%s", cmd, password)
	}
	cmd = fmt.Sprintf("%s --format_csv_delimiter='%s'", cmd, string(delim))
	cmd = fmt.Sprintf("%s --query 'INSERT INTO %s FORMAT %s' < %s", cmd, tablename, format, filename)
	// running clickhouse-client as a command chokes on --query
	c := exec.Command("bash", "-c", cmd)
	err = c.Run()
	return
}

//TODO think about how to load from a query
func InsertSql(table string, qry string) (err error) {
	return nil
}

// Export exports data from Input to a CSV file
func Export(rdr Input, nTarget int, separator rune) (err error) {
	// TODO: see if pre-converting to string makes this faster
	const newLine = '\n'

	fmt.Println("start reading", time.Now())
	data, err := rdr.Read(nTarget, true)
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		return
	}
	_ = os.Remove("/home/will/tmp/try.csv")
	f, err := os.Create("/home/will/tmp/try.csv")
	if err != nil {
		return
	}
	_ = f.Close()
	fmt.Println("start writing", time.Now())
	file, err := os.OpenFile("/home/will/tmp/try.csv", os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		return
	}
	for r := 0; r < len(data); r++ {
		for c := 0; c < len(data[r]); c++ {
			char := separator
			if c == len(data[r])-1 {
				char = newLine
			}
			switch v := data[r][c].(type) {
			case string:
				_, err = file.WriteString(fmt.Sprintf("'%s'%s", v, string(char)))
			case time.Time:
				//a := fmt.Sprintf("%s%s", v.Format("2006-01-02"), string(char))
				//fmt.Println(a)
				_, err = file.WriteString(fmt.Sprintf("%s%s", v.Format("2006-01-02"), string(char)))
			default:
				_, err = file.WriteString(fmt.Sprintf("%v%s", v, string(char)))

			}
			if err != nil {
				return
			}
		}
	}
	fmt.Println("done writing", time.Now())
	return nil
}

//TODO: consider converting all these to streaming....
