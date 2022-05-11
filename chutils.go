// Package chutils is a set of utilities designed to work with ClickHouse.
// The utilities are designed to facilitate import and export of data.
//
//The chutils package defines:
//   - An Input interface that reads data.
//   - A TableDef struct that specifies the structure of the input. Features include:
//       - The fields/types of a TableDef can be specified, or they can be imputed from the data.
//       - The corresponding CREATE TABLE statement can be built and issued.
//       - Checks of the range/values of fields as they are read.
//   - An Output interface that writes data.
//   - Concurrent execution of Input/Output interfaces
//
// The file package implements Input and Output for text files.
// The sql package implements Input and Output for SQL.
//
// These two packages can be mixed and matched for Input/Output.
//
// The general use pattern is
//   1. Read from Input
//   2. <Validate data>
//   3. Write to output
//
// Example uses
//
// 1. Load a CSV to ClickHouse -- Option 1 (see Example in package file)
//    a. Define a file Reader to point to the CSV.
//    b. Use Init to create the TableDef and then Impute to determine the fields and types.
//    c. Use the Create method of TableDef to create the ClickHouse table to populate.
//    d. Define a file Writer that points a temporary file.
//    e. Use chutils Export to create a temporary file that uses the Reader/Writer.
//    f. Use the Writer Insert method to issue a command to clickhouse-client to load the temporary file.
//
// 2. Load a CSV to ClickHouse -- Option 2 (see Example in package sql).
//    a. same as a, above.
//    b. same as b, above.
//    c. same as c, above.
//    d. Define an SQL Writer that points to the table to populate.
//    e. Use chutils Export to create a VALUES insert statement.
//    f. Use the Writer Insert statement to execute the Insert.
//
// 3. Insert to a ClickHouse table from a ClickHouse query -- Option 1.
//    a. Define an sql Reader to define the source query.
//    b. Use Init to define the TableDef and Create to create the output table.
//    c. Use Insert to execute the insert query. (Note, there is no data validation).
//
// 4. Insert to a ClickHouse table from a ClickHouse query -- Option 2.
//    a. Same as a, above.
//    b. Same as b, above.
//    c. Define an sql Writer that points to the table to populate.
//    d. Use chutils Export to create the VALUEs statement that is used to insert into the table.
//    e. Use the Writer Insert statement to execute the Insert. (Note, there *is* data validation).
package chutils

import (
	"database/sql"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"time"
)

// Missing values used when the user does not supply them
var (
	DateMissing  = time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC)
	IntMissing   = -1   //math.MaxInt32
	FloatMissing = -1.0 //math.MaxFloat64
)

// DateFormats are formats to try when guessing the format in Impute()
var DateFormats = []string{"2006-01-02", "2006-1-2", "2006/01/02", "2006/1/2", "20060102", "01022006",
	"01/02/2006", "1/2/2006", "01-02-2006", "1-2-2006", "200601", time.RFC3339}

// The Input interface specifies the requirments for reading source data.
type Input interface {
	Read(nTarget int, validate bool) (data []Row, valid []Valid, err error) // Read from the Input, possibly with validation
	Reset() error                                                           // Reset to beginning of source
	CountLines() (numLines int, err error)                                  // CountLines returns # of lines in source
	Seek(lineNo int) error                                                  // Seek moves to lineNo in source
	Close() error                                                           // Close the source
	TableSpec() *TableDef                                                   // TableSpec returns the TableDef for the source
}

// The Output interface specifies requirements for writing data.
type Output interface {
	Write(b []byte) (n int, err error) // Write byte array, n is # of bytes written
	Name() string                      // Name of output (file, table)
	Insert() error                     // Inserts into ClickHouse Table
	Separator() rune                   // Separator returns the field separator
	EOL() rune                         // EOL returns the End-of-line character
	Close() error                      // Close writer
}

// Connect holds the ClickHouse connect information
type Connect struct {
	Http     string // Http is "http" or "https"
	Host     string // Host is host IP
	User     string // User is ClickHouse user name
	Password string // Password is user's password
	*sql.DB         // ClickHouse database connector
}

func NewConnect(http string, host string, user string, password string) (con *Connect, err error) {
	var db *sql.DB
	err = nil
	con = &Connect{http, host, user, password, db}
	if con.DB, err = sql.Open("clickhouse", con.String()); err != nil {
		return
	}
	err = con.DB.Ping()
	return
}

// ClickHouse connect string
func (c Connect) String() string {
	return fmt.Sprintf("%s://%s:8123/?user=%s&password=%s", c.Http, c.Host, c.User, c.Password)
}

// ErrType enum specifies the different error types trapped
type ErrType int

const (
	ErrUnknown ErrType = 0 + iota
	ErrInput
	ErrOutput
	ErrFields
	ErrFieldCount
	ErrDateFormat
	ErrSeek
	ErrRWNum
	ErrStr
	ErrSQL
)

//go:generate stringer -type=ErrType
func (i ErrType) Error() string {
	return i.String()
}

// Wrapper wraps an ErrType with a specific error message.
func Wrapper(e error, text string) error {
	return fmt.Errorf("%v: %w", text, e)
}

// EngineType enum specifies ClickHouse engine types
type EngineType int

const (
	MergeTree EngineType = 0 + iota
	Memory
)

//go:generate stringer -type=EngineType

// ChType enum is supported ClickHouse field types.
type ChType int

const (
	ChUnknown     ChType = 0 + iota // Unknown: ClickHouse type is undetermined
	ChInt                           // Int: ClickHouse type is Integer
	ChString                        // String: ClickHouse type is String
	ChFixedString                   // FixedString
	ChFloat                         // Float: ClickHouse type is Float
	ChDate                          // Date: ClickHouse type is Date
)

func (t ChType) String() string {
	switch t {
	case ChUnknown:
		return "Unknown"
	case ChInt:
		return "Int"
	case ChString:
		return "String"
	case ChFixedString:
		return "FixedString"
	case ChFloat:
		return "Float"
	case ChDate:
		return "Date"
	}
	return ""
}

// ChField struct holds the specification for a ClickHouse field
type ChField struct {
	Base       ChType // Base is base type of ClickHouse field.
	Length     int    // Length is length of field (0 for String).
	OuterFunc  string // OuterFunc is the outer function applied to the field (e.g. LowCardinality(), Nullable())
	DateFormat string // Format for incoming dates from Input
}

// Converter method converts an arbitrary value to the ClickHouse type requested.
// Returns the value and a boolean indicating whether this was successful.
func (t ChField) Converter(inValue interface{}, missing interface{}) (outValue interface{}, ok bool) {
	if reflect.ValueOf(inValue).Kind() == reflect.Slice {
		ok = true
		it := &Iterator{data: inValue}
		for it.Next() {
			outV, ok1 := Convert(it.Item, t)
			if !ok1 {
				outV = missing
			}
			it.Append(outV)
			ok = ok && ok1
		}
		outValue = it.NewItems
		return
	}
	outValue, ok = Convert(inValue, t)
	return
}

// LegalValues holds bounds and lists of legal values for a ClickHouse field
type LegalValues struct {
	LowLimit  interface{}     // Minimum legal value for types Int, Float
	HighLimit interface{}     // Maximum legal value for types Int, Float
	FirstDate *time.Time      // Earliest legal date for Date
	LastDate  *time.Time      // Last legal date for type Date
	Levels    *map[string]int // Legal values for types String, FixedString
}

// NewLegalValues creates a new LegalValues type
func NewLegalValues() *LegalValues {
	x := make(map[string]int)
	return &LegalValues{Levels: &x}
}

// Check checks whether checkVal is a legal value
func (l *LegalValues) Check(checkVal interface{}, missing interface{}) (outVal interface{}, ok bool) {

	if reflect.ValueOf(checkVal).Kind() == reflect.Slice {
		ok = true
		it := &Iterator{data: checkVal}
		for it.Next() {
			ov, ok1 := l.Check(it.Item, missing)
			it.Append(ov)
			ok = ok && ok1
		}
		return it.NewItems, ok
	}
	ok, outVal = true, checkVal
	switch val := checkVal.(type) {
	case string:
		if l.Levels == nil || len(*l.Levels) == 0 {
			return
		}
		// check if this is supposed to be a numeric field.
		for rx := range *l.Levels {
			if val == rx {
				return
			}
		}
	case float64:
		if l.LowLimit == nil && l.HighLimit == nil {
			return
		}
		// if l.LowLimit and l.HighLimit aren't the correct type, then fail
		low, okLow := l.LowLimit.(float64)
		high, okHigh := l.HighLimit.(float64)
		if okLow && okHigh {
			if low == high {
				return
			}
			// Do range check
			if val >= low && val <= high {
				return
			}
		}
	case float32:
		if l.LowLimit == nil && l.HighLimit == nil {
			return
		}
		// if l.LowLimit and l.HighLimit aren't the correct type, then fail
		low, okLow := l.LowLimit.(float32)
		high, okHigh := l.HighLimit.(float32)
		if okLow && okHigh {
			if low == high {
				return
			}
			// Do range check
			if val >= low && val <= high {
				return
			}
		}
	case int64:
		if l.LowLimit == nil && l.HighLimit == nil {
			return
		}
		// if l.LowLimit and l.HighLimit aren't the correct type, then fail
		low, okLow := l.LowLimit.(int64)
		high, okHigh := l.HighLimit.(int64)
		if okLow && okHigh {
			if low == high {
				return
			}
			// Do range check
			if val >= low && val <= high {
				return
			}
		}
	case int32:
		if l.LowLimit == nil && l.HighLimit == nil {
			return
		}
		// if l.LowLimit and l.HighLimit aren't the correct type, then fail
		low, okLow := l.LowLimit.(int32)
		high, okHigh := l.HighLimit.(int32)
		if okLow && okHigh {
			if low == high {
				return
			}
			// Do range check
			if val >= low && val <= high {
				return
			}
		}
	case time.Time:
		if l.FirstDate == nil && l.LastDate == nil {
			return
		}
		// These will be the correct type
		if l.FirstDate != nil && l.LastDate != nil {
			if val.Sub(*l.FirstDate) >= 0 && (*l).LastDate.Sub(val) >= 0 {
				return
			}
		}
	}
	return missing, false
}

// FindType determines the ChType of newVal.  If the target type is already set, this is a noop.
// Otherwise, the order of precedence is: ChDate, ChInt, ChFloat, ChString.
// If it is a date, the date format is set in target.
func FindType(newVal string, target *ChField) (res ChType) {

	// if target != Unknown, then don't try anything
	res = target.Base

	// Figure out what type this newVal might be.
	// The order of assessing this is: Date, Int, Float, String
	if res == ChUnknown {
		// Assign to string first -- this always works
		res = ChString
		// float ?
		if _, err := strconv.ParseFloat(newVal, 64); err == nil {
			res = ChFloat
		}
		// int ?
		if _, err := strconv.ParseInt(newVal, 10, 64); err == nil {
			res = ChInt
		}
		// date?
		if dfmt, _, err := FindFormat(newVal); err == nil {
			target.DateFormat = dfmt
			res = ChDate
		}
	}
	return
}

// Row is single row of the table.  The fields may be of any type.
// A Row is stored in the same order of the TableDef FieldDefs slice.
type Row []interface{}

// Status enum is the validation status of a particular instance of a ChField field
// as judged against its ClickHouse type and acceptable values
type Status int

// Field Validation Status enum type
const (
	VPass      Status = 0 + iota // Pass: value is OK
	VValueFail                   // ValueFail: value is illegal
	VTypeFail                    // TypeFail: value cannot be coerced to correct type
)

//go:generate stringer -type=Status

// Valid is a slice of type Status that is returned by Read if validate=true
type Valid []Status

// FieldDef struct holds the full definition of single ClickHouse field.
type FieldDef struct {
	Name        string       // Name of the field.
	ChSpec      ChField      // ChSpec is the Clickhouse specification of field.
	Description string       // Description is an optional description for CREATE TABLE statement.
	Legal       *LegalValues // Legal are optional bounds/list of legal values.
	Missing     interface{}  // Missing is the value used for a field if the value is missing/illegal.
	//	Calculator  func(td *TableDef, r Row) interface{} // Calculator is an optional function to calculate the field when it is missing.
	Width int // Width of field (for flat files)
}

// Validator checks the value of the field (inValue) against its FieldDef.
// outValue is the inValue that has the correct type. It is set to its Missing value if the Validation fails.
func (fd *FieldDef) Validator(inValue interface{}) (outValue interface{}, status Status) {
	status = VPass
	outValue, ok := fd.ChSpec.Converter(inValue, fd.Missing)
	if !ok {
		status = VTypeFail
		outValue = fd.Missing
		return
	}

	if outValue, ok = fd.Legal.Check(outValue, fd.Missing); !ok {
		status = VValueFail
	}
	return
}

// TableDef struct defines a table.
type TableDef struct {
	Key       string            // Key is the key for the table.
	Engine    EngineType        // EngineType is the ClickHouse table engine.
	FieldDefs map[int]*FieldDef // Map of fields in the table. The int key is the column order in the table.
}

// Get returns the FieldDef for field "name".  The FieldDefs map is by column order, so access
// by field name is needed.
func (td *TableDef) Get(name string) (int, *FieldDef, error) {
	for ind, fdx := range td.FieldDefs {
		if fdx.Name == name {
			return ind, fdx, nil
		}
	}
	return 0, nil, Wrapper(ErrFields, name)
}

// FindFormat determines the date format for a date represented as a string.
func FindFormat(inDate string) (format string, date time.Time, err error) {
	format = ""
	for _, format = range DateFormats {
		date, err = time.Parse(format, inDate)
		if err == nil {
			return
		}
	}
	return "", DateMissing, Wrapper(ErrDateFormat, inDate)
}

// Impute looks at the data from Input reader and builds the FieldDefs.
// It expects each field in rdr to come in as string.
func (td *TableDef) Impute(rdr Input, rowsToExamine int, tol float64) (err error) {
	err = nil
	if err = rdr.Reset(); err != nil {
		return
	}
	defer func() { err = rdr.Reset() }()
	// countType keeps track of the field values as the file is read
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

	// Look at RowsToExamine rows to see what types we have.
	rowCount := 0
	for rowCount = 0; (rowCount < rowsToExamine) || rowsToExamine == 0; rowCount++ {
		data, _, errx := rdr.Read(1, false)
		// EOF is not an error -- just stop reading
		if errx == io.EOF {
			break
		}
		if errx != nil {
			return Wrapper(ErrInput, fmt.Sprintf("%d", rowCount))
		}
		for ind := 0; ind < len(data[0]); ind++ {
			var (
				fval string
				ok   bool
			)
			if fval, ok = data[0][ind].(string); ok != true {
				return Wrapper(ErrStr, fmt.Sprintf("%v %v", fval, rowCount))
			}
			// aggregate results for each field across the rows
			switch FindType(fval, &td.FieldDefs[ind].ChSpec) {
			case ChInt:
				counts[ind].ints++
			case ChFloat:
				counts[ind].floats++
			case ChDate:
				counts[ind].dates++
			}
		}
	}

	// Threshold to determine which type a field is (100*tol % agreement)
	thresh := int(math.Max(1.0, tol*float64(rowCount)))

	// Select field type.
	for ind := 0; ind < numFields; ind++ {
		fd := td.FieldDefs[ind]
		// only impute type if user has not specified it
		if fd.ChSpec.Base == ChUnknown {
			switch {
			case counts[ind].dates >= thresh:
				fd.ChSpec.Base, fd.ChSpec.Length = ChDate, 0
				fd.Legal.Levels, fd.Legal.HighLimit, fd.Legal.LowLimit = nil, nil, nil
				fd.Legal.FirstDate, fd.Legal.LastDate = counts[ind].legal.FirstDate, counts[ind].legal.LastDate
				fd.Missing = DateMissing
			case counts[ind].ints >= thresh:
				fd.ChSpec.Base, fd.ChSpec.Length = ChInt, 64
				fd.Missing = IntMissing
			case (counts[ind].ints + counts[ind].floats) >= thresh:
				// Some values may convert to int in the file -- these could also be floats
				td.FieldDefs[ind].ChSpec.Base, td.FieldDefs[ind].ChSpec.Length = ChFloat, 64
				td.FieldDefs[ind].Missing = FloatMissing
			default:
				fd.ChSpec.Base = ChString
				fd.Missing = "!"
			}
		}
		switch fd.ChSpec.Base {
		case ChInt:
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit = nil, int64(0), int64(0)
		case ChFloat:
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit = nil, 0.0, 0.0
		default:
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit = nil, nil, nil // counts[ind].legal.Levels, nil, nil
		}
	}
	return
}

// Create builds and issues CREATE TABLE ClickHouse statement. The table created is "table"
func (td *TableDef) Create(conn *Connect, table string) error {
	qry := fmt.Sprintf("DROP TABLE IF EXISTS %v", table)
	if _, err := conn.Exec(qry); err != nil {
		return err
	}
	qry = fmt.Sprintf("CREATE TABLE %v (", table)
	for ind := 0; ind < len(td.FieldDefs); ind++ {
		fd := td.FieldDefs[ind]
		// start by creating the ClickHouse type
		ftype := fmt.Sprintf("%v", fd.ChSpec.Base)
		switch fd.ChSpec.Base {
		case ChInt, ChFloat:
			ftype = fmt.Sprintf("%s%d", ftype, fd.ChSpec.Length)
		case ChFixedString:
			ftype = fmt.Sprintf("%s(%d)", ftype, fd.ChSpec.Length)
		}
		if fd.ChSpec.OuterFunc != "" {
			ftype = fmt.Sprintf("%s(%s)", fd.ChSpec.OuterFunc, ftype)
		}
		// Prepend field name.
		ftype = fmt.Sprintf("%s     %s", fd.Name, ftype)
		// add comment
		if fd.Description != "" {
			ftype = fmt.Sprintf("%s     comment '%s'", ftype, fd.Description)
		}
		// Determine trailing character.
		char := ","
		if ind == len(td.FieldDefs)-1 {
			char = ")"
		}
		ftype = fmt.Sprintf("%s%s\n", ftype, char)
		// Add to create query.
		qry = fmt.Sprintf("%s %s", qry, ftype)
	}
	qry = fmt.Sprintf("%s ENGINE=%v()\nORDER BY (%s)", qry, td.Engine, td.Key)
	_, err := conn.Exec(qry)
	return err
}

func writeElement(el interface{}, char string) []byte {
	switch v := el.(type) {
	case string:
		return []byte(fmt.Sprintf("'%s'%s", v, char))
	case time.Time:
		return []byte(fmt.Sprintf("'%s'%s", v.Format("2006-01-02"), char))
	case float64, float32:
		return []byte(fmt.Sprintf("%0.2f%s", v, char))
	default:
		return []byte(fmt.Sprintf("%v%s", v, char))
	}
}

func writeArray(el interface{}, char string) (line []byte) {
	line = []byte("array(")
	it := &Iterator{data: el}
	for it.Next() {
		line = append(line, writeElement(it.Item, ",")...)
	}
	line[len(line)-1] = ')'
	line = append(line, char...)
	return
}

// Export transfers the contents of rdr to wrtr.
func Export(rdr Input, wrtr Output) error {

	var data []Row
	for r := 0; ; r++ {
		var err error
		if data, _, err = rdr.Read(1, true); err != nil {
			// no need to report EOF
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return Wrapper(ErrInput, fmt.Sprintf("%v", r))
			}
			fmt.Println("done writing", time.Now())
		}
		line := make([]byte, 0)
		for c := 0; c < len(data[0]); c++ {
			char := string(wrtr.Separator())
			if c == len(data[0])-1 {
				char = string(wrtr.EOL())
				if wrtr.EOL() == 0 {
					char = ""
				}
			}
			if reflect.ValueOf(data[0][c]).Kind() == reflect.Slice {
				line = append(line, writeArray(data[0][c], char)...)
			} else {
				line = append(line, writeElement(data[0][c], char)...)
			}
		}
		if _, err = wrtr.Write(line); err != nil {
			return Wrapper(ErrInput, fmt.Sprintf("%v", r))
		}
	}
}

// Load reads lines from rdr, writes them to wrtr and finally Inserts the data into table.
func Load(rdr Input, wrtr Output) (err error) {
	err = Export(rdr, wrtr)
	if err != nil {
		return
	}
	err = wrtr.Insert()
	return
}

// Concur loads a ClickHouse table from an array of Inputs/Outputs concurrently.
func Concur(nWorker int, rdrs []Input, wrtrs []Output,
	f func(rdr Input, wrtr Output) error) error {
	start := time.Now()
	if nWorker == 0 {
		nWorker = runtime.NumCPU()
	}
	if len(rdrs) != len(wrtrs) {
		return Wrapper(ErrRWNum, fmt.Sprintf("%v %v", len(rdrs), len(wrtrs)))
	}
	queueLen := len(rdrs)
	if nWorker > queueLen {
		nWorker = queueLen
	}
	c := make(chan error)

	running := 0
	for ind := 0; ind < queueLen; ind++ {
		ind := ind // Required since the function below is a closure
		go func() {
			c <- f(rdrs[ind], wrtrs[ind])
			return
		}()
		running++
		if running == nWorker {
			e := <-c
			if e != nil {
				return e
			}
			running--
		}
	}
	// Wait for queue to empty
	for running > 0 {
		e := <-c
		if e != nil {
			return e
		}
		running--
	}
	for ind := 0; ind < queueLen; ind++ {
		if err := rdrs[ind].Close(); err != nil {
			return err
		}
	}
	elapsed := time.Now().Sub(start)
	fmt.Println("Elapsed time", elapsed.Seconds(), "seconds")
	return nil
}
