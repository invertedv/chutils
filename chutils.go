// Package chutils is a set of utilities designed to work with ClickHouse.
// The package supports:
//   - Importing data.
//   - Building and issuing the CREATE TABLE statement.
//   - QA that checks field
//   - User-supplied function that can calculate the field if it fails QA
//   - Concurrent execution of table loading
package chutils

//TODO: add read and write functions to TableDefs -- JSON? yes? no?
//TODO: add another slice of FieldDefs that are calculated fields

import (
	"database/sql"
	"fmt"
	"io"
	"math"
	"os/exec"
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

// DateFormats are formats to try when guessing the format
var DateFormats = []string{"2006-01-02", "2006-1-2", "2006/01/02", "2006/1/2", "20060102", "01022006",
	"01/02/2006", "1/2/2006", "01-02-2006", "1-2-2006", "200601"}

// The Input interface specifies the requirments for reading source data.
type Input interface {
	Read(nTarget int, validate bool) (data []Row, err error) // Read reads rows from the source
	Reset()                                                  // Reset to beginning of table
	CountLines() (numLines int, err error)                   // CountLines returns # of lines in source data
	Seek(lineNo int) error                                   // Seek moves to lineNo in source data
	Name() string                                            // Name of input (file, table, etc)
	Close() error                                            // Close the source
}

// The Output interface specifies requirements for writing data.
type Output interface {
	Write(b []byte) (n int, err error) // Write byte array, n is # of bytes written
	Name() string                      // Name of output (file, etc)
	Insert() error                     // Close the source
	Separator() string
	EOL() string
	Close() error
}

// Connect holds the ClickHouse connect information
type Connect struct {
	Host     string
	User     string
	Password string
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
)

//go:generate stringer -type=ErrType

// ChErr is the error handler for error messages
type ChErr struct {
	Err string
}

func (e *ChErr) Error() string {
	return e.Err
}

// NewChErr returns an error type. Debugging problems in input files is tedious -- so supply extra info where possible.
func NewChErr(base ErrType, p ...any) *ChErr {
	var errstr string
	switch base {
	case ErrInput:
		errstr = fmt.Sprintf("input error row %v", p[0])
	case ErrOutput:
		errstr = fmt.Sprintf("output error row %v", p[0])
	case ErrFieldCount:
		errstr = fmt.Sprintf("expected %v fields got %v field", p[0], p[1])
	case ErrSeek:
		errstr = fmt.Sprintf("error seeking row %v", p[0])
	case ErrDateFormat:
		errstr = fmt.Sprintf("unable to find a date format for %v", p[0])
	case ErrRWNum:
		errstr = fmt.Sprintf("%v inputs != %v outputs", p[0], p[1])
	case ErrStr:
		errstr = fmt.Sprintf("field %v is not type string", p[0])
	case ErrFields:
		errstr = fmt.Sprintf("fields: %v", p[0])
	default:
		errstr = "Unknown error"
	}
	return &ChErr{errstr}
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
	Unknown     ChType = 0 + iota // Unknown: ClickHouse type is undetermined
	Int                           // Int: ClickHouse type is Integer
	String                        // String: ClickHouse type is String
	FixedString                   // FixedString
	Float                         // Float: ClickHouse type is Float
	Date                          // Date: ClickHouse type is Date
)

//go:generate stringer -type=ChType

// ChField struct holds the specification for a ClickHouse field
type ChField struct {
	Base       ChType // Base is base type of ClickHouse field.
	Length     int    // Length is length of field (0 for String).
	OuterFunc  string // OuterFunc is the outer function applied to the field (e.g. LowCardinality(), Nullable())
	DateFormat string // Format for incoming dates from Input when dates come in as string.
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
			fmt.Println("FILL ME IN -- check will fit in type length")
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
		if val.Sub(*l.FirstDate) >= 0 && (*l).LastDate.Sub(val) >= 0 {
			return
		}
	}
	ok = false
	return
}

// Update takes a LegalValues struct and updates it with newVal value.
// If the target is an int/float field, it will update High/Low.
// If the target is a discrete field, it will add (if needed) the field to the map and update its count.
func (l *LegalValues) Update(newVal string, target *ChField) (res ChType) {

	// if target != Unknown, this will force the type indicated by target
	res = target.Base

	// Figure out what type this newVal might be.
	// The order of assessing this is: Date, Int, Float, String
	if res == Unknown {
		// LowLimit, HighLimit are float64. Converted to int later (if need be)
		res = String
		// float ?
		if _, err := strconv.ParseFloat(newVal, 64); err == nil {
			res = Float
		}
		// int ?
		if _, err := strconv.ParseInt(newVal, 10, 64); err == nil {
			res = Int
		}
		if _, _, err := FindFormat(newVal); err == nil {
			res = Date
		}
	}
	// Now update the legal values
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
	case String, FixedString:
		if l.Levels == nil {
			//			res = String
			x := make(map[string]int, 0)
			l.Levels = &x
		}
		(*l.Levels)[newVal]++
	case Date:
		var (
			vx  time.Time
			f   string
			err error
		)
		if target.DateFormat != "" {
			if vx, err = time.Parse(target.DateFormat, newVal); err != nil {
				return
			}
		} else {
			if f, vx, err = FindFormat(newVal); err != nil {
				return
			}
			target.DateFormat = f
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

// Row is single row of the table.  The fields may be of any type.
// A Row is stored in the same order of the TableDef FieldDefs slice.
type Row []interface{}

// Status enum is the validation status of a particular instance of a ChField field
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

// FieldDef struct holds the full definition of single ClickHouse field.
type FieldDef struct {
	Name        string                   // Name of the field.
	ChSpec      ChField                  // ChSpec is the Clickhouse specification of field.
	Description string                   // Description is an optional description for CREATE TABLE statement.
	Legal       *LegalValues             // Legal are optional bounds/list of legal values.
	Missing     interface{}              // Missing is the value used for a field if the value is missing/illegal.
	Calculator  func(fs Row) interface{} // Calculator is an optional function to calculate the field when it is missing.
	Width       int                      // Width of field (for flat files)
}

// Validator checks the value of the field (inValue) against its FieldDef.
// outValue is the inValue that has the correct type. It is set to its Missing value if the Validation fails.
func (fd *FieldDef) Validator(inValue interface{}, r Row, s Status) (outValue interface{}, status Status) {
	status = Pass
	outValue, ok := fd.ChSpec.Converter(inValue)
	if !ok {
		status = TypeFail
		outValue = fd.Missing
		return
	}

	// TODO: length check to include int, float, change Missing to Max value for length
	// check FixedString is not too long
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

// TableDef struct defines a table.
type TableDef struct {
	Name      string            // Name is the ClickHouse name of the table.
	Key       string            // Key is the key for the table.
	Engine    EngineType        // EngineType is the ClickHouse table engine.
	FieldDefs map[int]*FieldDef // Map of fields in the table. The int key is the column order in the table.
}

// Get returns the FieldDef for field "name", nil if there is not such a field.
// Since the map is by column order, this is handy to get the field by name.
func (td *TableDef) Get(name string) *FieldDef {
	for _, fdx := range td.FieldDefs {
		if fdx.Name == name {
			return fdx
		}
	}
	return nil
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
	return "", DateMissing, NewChErr(ErrDateFormat, inDate)
}

// Impute looks at the data from Input and builds the FieldDefs.
// It requires each field in rdr to come in as string.
func (td *TableDef) Impute(rdr Input, rowsToExamine int, tol float64, fuzz int) error {
	defer rdr.Reset()
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
			return NewChErr(ErrInput, rowCount)
		}
		for ind := 0; ind < len(data[0]); ind++ {
			var (
				fval string
				ok   bool
			)
			if fval, ok = data[0][ind].(string); ok != true {
				return NewChErr(ErrStr, td.Name)
			}
			switch counts[ind].legal.Update(fval, &td.FieldDefs[ind].ChSpec) {
			case Int:
				counts[ind].ints++
			case Float:
				counts[ind].floats++
			case Date:
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
		if fd.ChSpec.Base == Unknown {
			switch {
			case counts[ind].dates >= thresh:
				fd.ChSpec.Base, fd.ChSpec.Length = Date, 0
				fd.Legal.Levels, fd.Legal.HighLimit, fd.Legal.LowLimit = nil, nil, nil
				fd.Legal.FirstDate, fd.Legal.LastDate = counts[ind].legal.FirstDate, counts[ind].legal.LastDate
				fd.Missing = DateMissing
			case counts[ind].ints >= thresh:
				fd.ChSpec.Base, fd.ChSpec.Length = Int, 64
				fd.Missing = IntMissing
			case (counts[ind].ints + counts[ind].floats) >= thresh:
				// Some values may convert to int in the file -- these could also be floats
				td.FieldDefs[ind].ChSpec.Base, td.FieldDefs[ind].ChSpec.Length = Float, 64
				td.FieldDefs[ind].Missing = FloatMissing
			default:
				fd.ChSpec.Base = String
				fd.Missing = "!"
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
	return nil
}

// Create func builds and issues CREATE TABLE ClickHouse statement
func (td *TableDef) Create(db *sql.DB) error {
	//db should be database object
	qry := fmt.Sprintf("DROP TABLE IF EXISTS %v", td.Name)
	if _, err := db.Exec(qry); err != nil {
		return err
	}
	qry = fmt.Sprintf("CREATE TABLE %v (", td.Name)
	for ind := 0; ind < len(td.FieldDefs); ind++ {
		fd := td.FieldDefs[ind]
		// start by creating the ClickHouse type
		ftype := fmt.Sprintf("%v", fd.ChSpec.Base)
		switch fd.ChSpec.Base {
		case Int, Float:
			ftype = fmt.Sprintf("%s%d", ftype, fd.ChSpec.Length)
		case FixedString:
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
	_, err := db.Exec(qry)
	return err
}

// InsertData creates and issues a ClickHouse INSERT Statement
// Should only be used with small amounts of data
func (td *TableDef) InsertData(t Input, rowCount int, db *sql.DB) error {
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
	_, err := db.Exec(qry)
	return err
}

// FileFormat enum has supported file types for bulk insert
type FileFormat int

const (
	CSV FileFormat = 0 + iota
	CSVWithNames
	TabSeparated
)

//go:generate stringer -type=FileFormat

// InsertFile uses clickhouse-client to bulk insert a file
func xInsertFile(tablename string, filename string, delim rune, format FileFormat, options string, con Connect) error {

	cmd := fmt.Sprintf("clickhouse-client --host=%s --user=%s", con.Host, con.User)
	if con.Password != "" {
		cmd = fmt.Sprintf("%s --password=%s", cmd, con.Password)
	}
	cmd = fmt.Sprintf("%s %s ", cmd, options)
	cmd = fmt.Sprintf("%s --format_csv_delimiter='%s'", cmd, string(delim))
	cmd = fmt.Sprintf("%s --query 'INSERT INTO %s FORMAT %s' < %s", cmd, tablename, format, filename)
	// running clickhouse-client as a command chokes on --query
	c := exec.Command("bash", "-c", cmd)
	err := c.Run()
	return err
}

//TODO think about how to load from a query.  Can Export be used???

//func InsertSql(table string, qry string) (err error) {
//	return nil
//}

// Export transfers the contents of rdr to wrtr.  Typically, wrtr will be a file.
func Export(rdr Input, wrtr Output, separator string, eol string) error {

	var data []Row
	for r := 0; ; r++ {
		var err error
		if data, err = rdr.Read(1, true); err != nil {
			// no need to report EOF
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return NewChErr(ErrInput, r)
			}
			fmt.Println("done writing", time.Now())
		}
		line := make([]byte, 0)
		for c := 0; c < len(data[0]); c++ {
			char := separator
			if c == len(data[0])-1 {
				char = eol
			}
			switch v := data[0][c].(type) {
			case string:
				line = append(line, []byte(fmt.Sprintf("'%s'%s", v, char))...)
			case time.Time:
				line = append(line, []byte(fmt.Sprintf("%s%s", v.Format("2006-01-02"), char))...)
			default:
				line = append(line, []byte(fmt.Sprintf("%v%s", v, char))...)
			}
		}
		if _, err = wrtr.Write(line); err != nil {
			return NewChErr(ErrOutput, r)
		}
	}
}

// TODO: think about whether to generalize this

// Load reads n lines from rdr, writes them to wrtr and finally inserts the data into table.
func Load(rdr Input, wrtr Output) (err error) {
	err = Export(rdr, wrtr, wrtr.Separator(), wrtr.EOL())
	if err != nil {
		return
	}
	err = wrtr.Insert()
	//	wrtr.Close()
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
		return NewChErr(ErrRWNum, len(rdrs), len(wrtrs))
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
