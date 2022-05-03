// Package chutils is a set of utilities designed to work with ClickHouse.
// The package supports:
//   - Importing data.
//   - Building and issuing the CREATE TABLE statement.
//   - QA that checks field
//   - User-supplied function that can calculate the field if it fails QA
//   - Concurrent execution of table loading
package chutils

//TODO: add another slice of FieldDefs that are calculated fields
//TODO: do I need file format?

import (
	"database/sql"
	"fmt"
	"io"
	"math"
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
	Read(nTarget int, validate bool) (data []Row, err error) // Read reads rows from the source
	Reset() error                                            // Reset to beginning of source
	CountLines() (numLines int, err error)                   // CountLines returns # of lines in source
	Seek(lineNo int) error                                   // Seek moves to lineNo in source
	Name() string                                            // Name of input (file, table, etc)
	Close() error                                            // Close the source
	Separator() rune
	EOL() rune
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

// Stringer which strips the leading Ch from the type.
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
func (t ChField) Converter(inValue interface{}) (outValue interface{}, ok bool) {
	var err error
	outValue = inValue
	switch t.Base {
	case ChString, ChFixedString:
		switch x := inValue.(type) {
		case float64, float32, int:
			outValue = fmt.Sprintf("%v", x)
		}
		if t.Base == ChFixedString && len(inValue.(string)) > t.Length {
			return nil, false
		}
	case ChFloat:
		switch x := inValue.(type) {
		case string:
			outValue, err = strconv.ParseFloat(x, t.Length)
			if err != nil {
				return nil, false
			}
		case int:
			fmt.Println("FILL ME IN -- check will fit in type length")
		}
	case ChInt:
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
	case ChDate:
		switch x := (inValue).(type) {
		case string:
			var outDate time.Time
			outDate, err = time.Parse(t.DateFormat, x)
			outValue = outDate //changed 4/29 outDate.Format("2006-01-02")
			if err != nil {
				return nil, false
			}
		case float64, float32:
			fmt.Println("FILL ME IN")
		}
	}
	return outValue, true
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
func (l *LegalValues) Check(checkVal interface{}) (ok bool) {
	ok = true
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
			// Do range check
			if val >= low && val <= high {
				return
			}
		}
	case int:
		if l.LowLimit == nil && l.HighLimit == nil {
			return
		}
		// if l.LowLimit and l.HighLimit aren't the correct type, then fail
		low, okLow := l.LowLimit.(int)
		high, okHigh := l.HighLimit.(int)
		if okLow && okHigh {
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
	ok = false
	return
}

// FindType takes a LegalValues struct and updates it with newVal value.
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
	VPending    Status = 0 + iota // Pending means the validation status is not determined
	VValueFail                    // ValueFail: Value is illegal
	VTypeFail                     // TypeFail: value cannot be coerced to correct type
	VCalculated                   // Calculated: value is calculated from other fields
	VPass                         // Pass: Value is OK
)

//go:generate stringer -type=Status

// FieldDef struct holds the full definition of single ClickHouse field.
type FieldDef struct {
	Name        string                                // Name of the field.
	ChSpec      ChField                               // ChSpec is the Clickhouse specification of field.
	Description string                                // Description is an optional description for CREATE TABLE statement.
	Legal       *LegalValues                          // Legal are optional bounds/list of legal values.
	Missing     interface{}                           // Missing is the value used for a field if the value is missing/illegal.
	Calculator  func(td *TableDef, r Row) interface{} // Calculator is an optional function to calculate the field when it is missing.
	Width       int                                   // Width of field (for flat files)
}

// Validator checks the value of the field (inValue) against its FieldDef.
// outValue is the inValue that has the correct type. It is set to its Missing value if the Validation fails.
func (fd *FieldDef) Validator(inValue interface{}, td *TableDef, r Row, s Status) (outValue interface{}, status Status) {
	status = VPass
	outValue, ok := fd.ChSpec.Converter(inValue)
	if !ok {
		status = VTypeFail
		outValue = fd.Missing
		return
	}

	if fd.Legal.Check(outValue) {
		return
	}
	if fd.Calculator != nil && s != VCalculated {
		hold := outValue
		outValue, status = fd.Validator(fd.Calculator(td, r), td, r, VCalculated)
		// see if estimated value is legal
		if status == VPass {
			status = VCalculated
			return
		}
		// if not, put back original value and Fail
		outValue = hold
	}
	outValue = fd.Missing
	status = VValueFail
	return
}

// TableDef struct defines a table.
type TableDef struct {
	Key       string            // Key is the key for the table.
	Engine    EngineType        // EngineType is the ClickHouse table engine.
	FieldDefs map[int]*FieldDef // Map of fields in the table. The int key is the column order in the table.
}

// Get returns the FieldDef for field "name", nil if there is not such a field.
// Since the map is by column order, this is handy to get the field by name.
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

// Impute looks at the data from Input and builds the FieldDefs.
// It requires each field in rdr to come in as string.
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
		data, errx := rdr.Read(1, false)
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
			// Convert LowLimit, HighLimit to int
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit = nil, 0, 0
			//				nil, int(counts[ind].legal.LowLimit.(float64)), int(counts[ind].legal.HighLimit.(float64))
		case ChFloat:
			// Some values may convert to int in the file -- these could also be floats
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit = nil, 0.0, 0.0
			//				nil, counts[ind].legal.LowLimit, counts[ind].legal.HighLimit
		default:
			fd.Legal.Levels, fd.Legal.LowLimit, fd.Legal.HighLimit = nil, nil, nil // counts[ind].legal.Levels, nil, nil
		}

	}
	return
}

// Create func builds and issues CREATE TABLE ClickHouse statement
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

// Export transfers the contents of rdr to wrtr.
func Export(rdr Input, wrtr Output) error {

	var data []Row
	for r := 0; ; r++ {
		var err error
		if data, err = rdr.Read(1, true); err != nil {
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
				char = ""
				if wrtr.EOL() != 0 {
					char = string(wrtr.EOL())
				}
			}
			switch v := data[0][c].(type) {
			case string:
				line = append(line, []byte(fmt.Sprintf("'%s'%s", v, char))...)
			case time.Time:
				line = append(line, []byte(fmt.Sprintf("'%s'%s", v.Format("2006-01-02"), char))...)
			default:
				line = append(line, []byte(fmt.Sprintf("%v%s", v, char))...)
			}
		}
		if _, err = wrtr.Write(line); err != nil {
			return Wrapper(ErrInput, fmt.Sprintf("%v", r))
		}
	}
}

// Load reads n lines from rdr, writes them to wrtr and finally inserts the data into table.
func Load(rdr Input, wrtr Output) (err error) {
	err = Export(rdr, wrtr)
	if err != nil {
		return
	}
	err = wrtr.Insert()
	return
}

// Concur loads a ClickHouse table from an array of Inputs/Outputs concurrently.
// wrtrs must produce a file that can be bulk imported to ClickHouse.
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
