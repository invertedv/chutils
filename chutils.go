// Package chutils is a set of utilities designed to work with ClickHouse.
// The utilities are designed to facilitate import and export of data.
//
// The chutils package facilitates these types of functions. The principal use cases are:
//
//  1. file --> ClickHouse
//  2. ClickHouse --> file
//  3. ClickHouse --> ClickHouse
//
// Why is use case 3 helpful?
//   - Automatic generation of the CREATE TABLE statement
//   - Data cleaning
//   - Renaming fields
//   - Adding fields that may be complex functions of the Input and/or use data from other Go variables.
//
// The chutils package defines:
//   - An Input interface that reads data.
//   - A TableDef struct that specifies the structure of the input. Features include:
//   - The fields/types of a TableDef can be specified, or they can be imputed from the data.
//   - The corresponding CREATE TABLE statement can be built and issued.
//   - Checks of the range/values of fields as they are read.
//   - An Output interface that writes data.
//   - Concurrent execution of Input/Output interfaces
//
// The file package implements Input and Output for text files.
// The sql package implements Input and Output for SQL.
//
// These two packages can be mixed and matched for Input/Output.
//
// # Example uses
//
//  1. Load a CSV to ClickHouse -- Option 1 (see Example in package file)
//     a. Define a file Reader to point to the CSV.
//     b. Use Init to create the TableDef and then Impute to determine the fields and types.
//     c. Use the Create method of TableDef to create the ClickHouse table to populate.
//     d. Run TableSpecs().Check() to verify the TableSpec is set up correctly.
//     e. Define a file Writer that points a temporary file.
//     f. Use chutils Export to create a temporary file that uses the Reader/Writer.
//     g. Use the Writer Insert method to issue a command to clickhouse-client to load the temporary file.
//
//  2. Load a CSV to ClickHouse -- Option 2 (see Example in package sql).
//     a. same as a, above.
//     b. same as b, above.
//     c. same as c, above.
//     d. same as d, above.
//     e. Define an SQL Writer that points to the table to populate.
//     f. Use chutils Export to create a VALUES insert statement.
//     g. Use the Writer Insert statement to execute the Insert.
//
//  3. Insert to a ClickHouse table from a ClickHouse query -- Option 1.
//     a. Define an sql Reader to define the source query.
//     b. Use Init to define the TableDef and Create to create the output table.
//     c. Run TableSpec().Check() to make sure the TableSpec is set up correctly.
//     d. Use Insert to execute the insert query. (Note, there is no data validation).
//
//  4. Insert to a ClickHouse table from a ClickHouse query -- Option 2.
//     a. Same as a, above.
//     b. Same as b, above.
//     c. Run TableSpec().Check() to make sure the TableSpec is set up correctly.
//     d. Define an sql Writer that points to the table to populate.
//     e. Use chutils Export to create the VALUEs statement that is used to insert into the table.
//     f. Use the Writer Insert statement to execute the Insert. (Note, there *is* data validation).
package chutils

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Missing values used when the user does not supply them
var (
	DateMissing   = time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC)
	IntMissing    = -1
	FloatMissing  = -1.0
	StringMissing = "!"
)

// DateFormats are formats to try when guessing the field type in Impute()
var DateFormats = []string{"2006-01-02", "2006-1-2", "2006/01/02", "2006/1/2", "20060102", "01022006",
	"01/02/2006", "1/2/2006", "01-02-2006", "1-2-2006", "200601", "Jan 2 2006", "January 2 2006",
	"Jan 2, 2006", "January 2, 2006", time.RFC3339}

// The Input interface specifies the requirements for reading source data.
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
	Write(b []byte) (n int, err error) // Write byte array, n is # of bytes written. Writes do not go to ClickHouse (see Insert)
	Name() string                      // Name of output (file, table)
	Insert() error                     // Inserts into ClickHouse
	Separator() rune                   // Separator returns the field separator
	EOL() rune                         // EOL returns the End-of-line character
	Text() string                      // Text is text qualifier for strings.  If it is found, Export doubles it. For CH, this is a single quote.
	Close() error                      // Close writer
}

// Connect holds the ClickHouse connect information
type Connect struct {
	Host     string        // Host is host IP
	User     string        // User is ClickHouse username
	Password string        // Password is user's password
	timeOut  time.Duration // optional timeout parameter
	*sql.DB                // ClickHouse database connector
}

// Execute executes qry with timeOut (if supplied), otherwise uses .Exec
func (conn *Connect) Execute(qry string) error {
	if conn.timeOut == 0 {
		_, err := conn.Exec(qry)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), conn.timeOut*time.Minute)
	defer cancel()

	_, err := conn.ExecContext(ctx, qry)

	return err
}

// ConnectOpt is the type of function to set options for Connect
type ConnectOpt func(conn *Connect)

func WithTimeOut(timeOutMinutes int64) ConnectOpt {
	fn := func(conn *Connect) {
		conn.timeOut = time.Duration(timeOutMinutes)
	}

	return fn
}

// NewConnect established a new connection to ClickHouse.
// host is IP address (assumes port 9000), memory is max_memory_usage
func NewConnect(host, user, password string, settings clickhouse.Settings, opts ...ConnectOpt) (con *Connect, err error) {
	var db *sql.DB
	con = &Connect{Host: host, User: user, Password: password, DB: db, timeOut: 0}
	con.DB = clickhouse.OpenDB(
		&clickhouse.Options{
			Addr: []string{host + ":9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: user,
				Password: password,
			},
			Settings:    settings,
			DialTimeout: 300 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
				Level:  0,
			},
		})

	for _, opt := range opts {
		opt(con)
	}

	return con, con.DB.Ping()
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

type OuterFunc int

const (
	OuterArray = 0 + iota
	OuterLowCardinality
	OuterNullable
)

func (o OuterFunc) String() string {
	switch o {
	case OuterArray:
		return "Array"
	case OuterLowCardinality:
		return "LowCardinality"
	case OuterNullable:
		return "Nullable"
	}
	return ""
}

// OuterFuncs is a slice of OuterFunc since we can have multiple of these in a field
type OuterFuncs []OuterFunc

func (o OuterFuncs) Has(target OuterFunc) bool {
	if len(o) == 0 {
		return false
	}
	for _, t := range o {
		if t == target {
			return true
		}
	}
	return false
}

// ChField struct holds the specification for a ClickHouse field
type ChField struct {
	Base   ChType     // Base is base type of ClickHouse field.
	Length int        // Length is length of field (0 for String).
	Funcs  OuterFuncs // OuterFunc is the outer function applied to the field (e.g. LowCardinality(), Nullable())
	Format string     // Format for incoming dates from Input, or outgoing Floats
}

func (ch ChField) String() string {
	ret := ""
	cls := ""
	targets := []OuterFunc{OuterArray, OuterLowCardinality, OuterNullable}
	if len(ch.Funcs) > 0 {
		for j := 0; j < len(targets); j++ {
			if ch.Funcs.Has(targets[j]) {
				ret += fmt.Sprintf("%v(", targets[j])
				cls += ")"
			}
		}
	}

	chf := ""
	switch ch.Base {
	case ChString:
		chf = "String"
	case ChDate:
		chf = "Date"
	case ChFloat:
		chf = fmt.Sprintf("Float%d", ch.Length)
	case ChInt:
		chf = fmt.Sprintf("Int%d", ch.Length)
	case ChFixedString:
		chf = fmt.Sprintf("FixedString(%d)", ch.Length)
	default:
		chf = "error"
	}
	return ret + chf + cls
}

// Converter method converts an arbitrary value to the ClickHouse type requested.
// Returns the value and a boolean indicating whether this was successful.
// If the conversion is unsuccessful, the return value is "missing"
func (ch ChField) Converter(inValue, missing, deflt interface{}) (interface{}, Status) {
	// if there is a default value, see if we need it
	if deflt != nil {
		if inValue == nil {
			return deflt, VDefault
		}
		// check if the field is empty
		if asStr, okx := inValue.(string); okx {
			if asStr == "" {
				return deflt, VDefault
			}
		}
	}

	outValue, ok := convert(inValue, ch)
	if !ok {
		return missing, VTypeFail
	}

	return outValue, VPass
}

// LegalValues holds bounds and lists of legal values for a ClickHouse field
type LegalValues struct {
	LowLimit  interface{} // Minimum legal value for types Int, Float
	HighLimit interface{} // Maximum legal value for types Int, Float
	Levels    []string    // Legal values for types String, FixedString
}

// NewLegalValues creates a new LegalValues type
func NewLegalValues() *LegalValues {
	x := make([]string, 0)
	return &LegalValues{Levels: x}
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
		if dfmt, _, err := findFormat(newVal); err == nil {
			target.Format = dfmt
			res = ChDate
		}
	}

	return res
}

// Row is single row of the table.  The fields may be of any type.
// A Row is stored in the same order of the TableDef FieldDefs slice.
type Row []interface{}

// Status enum is the validation status of a particular instance of a ChField field
// as judged against its ClickHouse type and acceptable values
type Status int

// Field Validation Status enum type
const (
	VPass      Status = 0 + iota // VPass: value is OK
	VDefault                     // VDefault: default value was used because field was empty
	VValueFail                   // ValueFail: value is illegal
	VTypeFail                    // VTypeFail: value cannot be coerced to correct type
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
	Missing     interface{}  // Missing is the value used for a field if the value is illegal.
	Default     interface{}  // Default is the value used for a field if the field is empty
	Width       int          // Width of field (for flat files)
	Drop        bool         // Drop, if True, instructs Outputs to ignore this field
}

func NewFieldDef(name string, chSpec ChField, description string, legal *LegalValues, missing interface{}, width int) *FieldDef {
	fd := &FieldDef{
		Name:        name,
		ChSpec:      chSpec,
		Description: description,
		Legal:       legal,
		Missing:     missing,
		Width:       width,
	}
	_ = fd
	return fd
}

// CheckRange checks whether checkVal is a legal value. Returns fd.Missing, if not.
func (fd *FieldDef) CheckRange(checkVal interface{}) (interface{}, Status) {
	switch val := checkVal.(type) {
	case string:
		if fd.ChSpec.Base == ChFixedString {
			if len(val) > fd.ChSpec.Length {
				return fd.Missing, VTypeFail
			}
		}
		// nothing to do?
		if fd.Legal.Levels == nil || len(fd.Legal.Levels) == 0 {
			return checkVal, VPass
		}
		// check if this is supposed to be a numeric field.
		for _, rx := range fd.Legal.Levels {
			if val == rx {
				return checkVal, VPass
			}
		}
	default:
		if compare(checkVal, fd.Legal.LowLimit, fd.ChSpec) && compare(fd.Legal.HighLimit, checkVal, fd.ChSpec) {
			return checkVal, VPass
		}
	}
	return fd.Missing, VValueFail
}

// Validator checks the value of the field (inValue) against its FieldDef.
// outValue is the inValue that has the correct type. It is set to its fd.Missing if the Validation fails.
func (fd *FieldDef) Validator(inValue interface{}) (outVal interface{}, status Status) {
	if reflect.ValueOf(inValue).Kind() == reflect.Slice {
		statusTotal := VPass
		it := newIterator(inValue)

		for it.Next() {
			ov, stat := fd.Validator(it.Item)
			it.Append(ov)
			if stat != VPass {
				statusTotal = VValueFail
			}
		}

		return it.NewItems, statusTotal
	}

	// convert to correct type (if needed)
	if outVal, status = fd.ChSpec.Converter(inValue, fd.Missing, fd.Default); status != VPass {
		return
	}
	// check against ranges/levels
	return fd.CheckRange(outVal)
}

// TableDef struct defines a table.
type TableDef struct {
	Key       string            // Key is the key for the table.
	Engine    EngineType        // EngineType is the ClickHouse table engine.
	FieldDefs map[int]*FieldDef // Map of fields in the table. The int key is the column order in the table.
	nested    map[string][2]int // Map of nested fields. String is nested name, [2]int is first & last index into FieldDefs
}

// NewTableDef creates a new TableDef struc
func NewTableDef(key string, engine EngineType, fielddefs map[int]*FieldDef) *TableDef {
	td := &TableDef{
		Key:       key,
		Engine:    engine,
		FieldDefs: fielddefs,
		nested:    nil,
	}

	return td
}

// Nest defines a set of fields in [field1, field2] as being nested with name nestName
func (td *TableDef) Nest(nestName, firstField, lastField string) error {
	_, ok := td.nested[nestName]
	if ok {
		return Wrapper(ErrFields, fmt.Sprintf("nested name %s already exists", nestName))
	}

	if firstField == lastField {
		return Wrapper(ErrFields, "nests must have at least two fields")
	}

	ind1, _, err := td.Get(firstField)
	if err != nil {
		return err
	}

	ind2, _, err := td.Get(lastField)
	if err != nil {
		return err
	}

	if ind2 < ind1 {
		ind1, ind2 = ind2, ind1
	}

	// check all nested fields are arrays and remove array outer function -- it is implied for nested fields
	for ind := ind1; ind <= ind2; ind++ {
		fd := td.FieldDefs[ind]
		if !fd.ChSpec.Funcs.Has(OuterArray) {
			return Wrapper(ErrFields, fmt.Sprintf("field %s must be an array to be nested", fd.Name))
		}

		var newOuter OuterFuncs = nil

		for _, fn := range fd.ChSpec.Funcs {
			if fn != OuterArray {
				newOuter = append(newOuter, fn)
			}
		}
		fd.ChSpec.Funcs = newOuter
	}

	if td.nested != nil {
		for k, v := range td.nested {
			if v[1] >= ind1 && v[0] <= ind2 {
				return Wrapper(ErrFields, fmt.Sprintf("nest %s would overlap nest %s", nestName, k))
			}
		}
	}

	if td.nested == nil {
		td.nested = make(map[string][2]int)
	}

	td.nested[nestName] = [2]int{ind1, ind2}

	return nil
}

// isNested returns the nest name for the first field in the nest and ")" for the last field
func (td *TableDef) isNested(ind int) (start, end string) {
	for k, v := range td.nested {
		if ind == v[0] {
			return k, "" // fmt.Sprintf("%s Nested(\n", k), ""
		}
		if ind == v[1] {
			return "", ")"
		}
	}

	return "", ""
}

// Get returns the FieldDef for field "name". The FieldDefs map is by column order, so access
// by field name is needed.
func (td *TableDef) Get(name string) (int, *FieldDef, error) {
	for ind, fdx := range td.FieldDefs {
		if fdx.Name == name {
			return ind, fdx, nil
		}
	}

	return 0, nil, Wrapper(ErrFields, name)
}

// findFormat determines the date format for a date represented as a string.
func findFormat(inDate string) (format string, date time.Time, err error) {
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
	if err = rdr.Reset(); err != nil {
		return
	}
	defer func() { _ = rdr.Reset() }()

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
			return Wrapper(ErrInput, fmt.Sprintf("error reading row %d", rowCount))
		}
		for ind := 0; ind < len(data[0]); ind++ {
			var (
				fval string
				ok   bool
			)

			if fval, ok = data[0][ind].(string); !ok {
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
				fd.Missing = StringMissing
			}
		}
	}
	// check our work
	return td.Check()
}

// Create builds and issues CREATE TABLE ClickHouse statement. The table created is "table"
func (td *TableDef) Create(conn *Connect, table string) error {
	// drop table if it exists
	qry := fmt.Sprintf("DROP TABLE IF EXISTS %v", table)
	if _, err := conn.Exec(qry); err != nil {
		return err
	}
	if td.Key == "" {
		return Wrapper(ErrFields, "TableSpecs().Key is empty")
	}

	// build CREATE TABLE statement
	qry = fmt.Sprintf("CREATE TABLE %v (", table)
	alter := make([]string, 0)
	isNested := false
	currentNest := ""

	// remove any dropped fields
	for ind := 0; ind < len(td.FieldDefs); ind++ {
		fd := td.FieldDefs[ind]
		if fd.Drop {
			continue
		}

		nestName, nestEnd := td.isNested(ind)
		nestStr := ""

		if nestName != "" {
			nestStr = fmt.Sprintf("%s Nested(\n", nestName)
			currentNest = nestName
		}

		isNested = isNested || (nestName != "")
		ftype := fmt.Sprintf("%s %s %v", nestStr, fd.Name, fd.ChSpec)

		// add comment
		if fd.Description != "" {
			if !isNested {
				ftype = fmt.Sprintf("%s     comment '%s'", ftype, fd.Description)
			} else {
				alter = append(alter, fmt.Sprintf("ALTER TABLE %s comment COLUMN %s.%s '%s'", table, currentNest,
					fd.Name, fd.Description))
			}
		}

		ftype = fmt.Sprintf("%s%s,\n", ftype, nestEnd)
		isNested = isNested && nestEnd != ")"
		// Add to create query.
		qry = fmt.Sprintf("%s %s", qry, ftype)
	}

	q := []byte(qry)
	q[len(q)-2] = ')'
	qry = fmt.Sprintf("%s ENGINE=%v()\nORDER BY (%s)", q, td.Engine, td.Key)

	if _, err := conn.Exec(qry); err != nil {
		return err
	}

	// Putting a comment in a CREATE TABLE for a nested field throws an error in ClickHouse
	for _, qry := range alter {
		if _, err := conn.Exec(qry); err != nil {
			return err
		}
	}

	return nil
}

func checkLen(fd *FieldDef) error {
	if fd.ChSpec.Base != ChFloat && fd.ChSpec.Base != ChInt {
		return nil
	}

	if fd.ChSpec.Length != 32 && fd.ChSpec.Length != 64 {
		return Wrapper(ErrFields,
			fmt.Sprintf("Floats and Ints must have length 32 or 64, field %s", fd.Name))
	}
	return nil
}

func checkConst(fd *FieldDef, val interface{}) (ret interface{}, err error) {
	if val == nil {
		return nil, nil
	}
	var (
		x  interface{}
		ok bool
	)

	if x, ok = convert(val, fd.ChSpec); !ok {
		return nil, Wrapper(ErrFields,
			fmt.Sprintf("cannot coerce value %v to %v, field: %s", fd.Missing, val, fd.Name))
	}

	return x, nil
}

// Check verifies that the fields are of the type chutils supports.  Check also converts, if needed, the
// Legal.HighLimit and Legal.LowLimit and Missing interfaces to the correct type and checks LowLimit <= HighLimit.
// It's a good idea to run Check before reading the data.
func (td *TableDef) Check() error {
	// see if key(s) are in the table
	for _, k := range strings.Split(td.Key, ",") {
		if _, _, e := td.Get(strings.Trim(k, " ")); e != nil {
			return Wrapper(ErrFields, fmt.Sprintf("key %s is not in the table", td.Key))
		}
	}

	// work through the fields, validate types, ranges/levels and Missing values.
	for _, fd := range td.FieldDefs {
		// Check OuterFunc is legit
		switch t := fd.ChSpec.Base; t {
		case ChFixedString:
			if fd.ChSpec.Length == 0 {
				return Wrapper(ErrFields, fmt.Sprintf("FixedString must have a length, field: %s", fd.Name))
			}
		case ChFloat, ChInt, ChDate:
			if e := checkLen(fd); e != nil {
				return e
			}

			v, e := checkConst(fd, fd.Missing)
			if e != nil {
				return e
			}
			fd.Missing = v

			v, e = checkConst(fd, fd.Default)
			if e != nil {
				return e
			}

			fd.Default = v

			if fd.Legal == nil {
				fd.Legal = &LegalValues{}
				continue
			}

			v, e = checkConst(fd, fd.Legal.HighLimit)
			if e != nil {
				return e
			}

			fd.Legal.HighLimit = v

			v, e = checkConst(fd, fd.Legal.LowLimit)
			if e != nil {
				return e
			}

			fd.Legal.LowLimit = v

			if !compare(fd.Legal.HighLimit, fd.Legal.LowLimit, fd.ChSpec) {
				return Wrapper(ErrFields,
					fmt.Sprintf("LowLimit %v >= HighLimit %v, field: %s", fd.Legal.LowLimit, fd.Legal.HighLimit, fd.Name))
			}
		case ChUnknown:
			return Wrapper(ErrFields, fmt.Sprintf("invalid ClickHouse type (ChUnknown), field: %s", fd.Name))
		}
	}
	return nil
}

// Copy makes an independent (no shared memory) of the TableDef.  if noLegal=true, then
// the LegalValues are not copied over (so there would be no validation check)
func (td *TableDef) Copy(noLegal bool) *TableDef {
	fdsOut := make(map[int]*FieldDef)

	for ind, fdIn := range td.FieldDefs {
		fdOut := &FieldDef{
			Name:        fdIn.Name,
			ChSpec:      fdIn.ChSpec,
			Description: fdIn.Description,
			Legal:       fdIn.Legal,
			Missing:     fdIn.Missing,
			Default:     fdIn.Default,
			Width:       fdIn.Width,
			Drop:        fdIn.Drop,
		}

		if noLegal {
			fdOut.Legal = &LegalValues{}
		}

		fdsOut[ind] = fdOut
	}

	return NewTableDef(td.Key, td.Engine, fdsOut)
}

// FieldList returns a slice of field names in the same order they are in the data
func (td *TableDef) FieldList() []string {
	var fieldList []string

	fds := td.FieldDefs
	for ind := 0; ind < len(fds); ind++ {
		fieldList = append(fieldList, fds[ind].Name)
	}

	return fieldList
}

// WriteElement writes a single field with separator char. For strings, the text qualifier is sdelim.
// If sdelim is found, it is doubled.
func WriteElement(el interface{}, char, sdelim string) []byte {
	if el == nil {
		return []byte(fmt.Sprintf("array()%s", char))
	}

	switch v := el.(type) {
	case string:
		if strings.Contains(v, sdelim) {
			return []byte(fmt.Sprintf("'%s'%s", strings.ReplaceAll(v, sdelim, sdelim+sdelim), char))
		}
		return []byte(fmt.Sprintf("%s%s%s%s", sdelim, v, sdelim, char))
	case time.Time:
		return []byte(fmt.Sprintf("%s%s%s%s", sdelim, v.Format("2006-01-02"), sdelim, char))
	case float64, float32:
		return []byte(fmt.Sprintf("%v%s", v, char))
	default:
		return []byte(fmt.Sprintf("%v%s", v, char))
	}
}

// WriteArray writes a ClickHouse Array type. The format is "array(a1,a2,...)"
func WriteArray(el interface{}, char, sdelim string) (line []byte) {
	line = []byte("array(")
	it := newIterator(el)
	for it.Next() {
		line = append(line, WriteElement(it.Item, ",", sdelim)...)
	}
	line[len(line)-1] = ')'
	line = append(line, char...)
	return
}

// Export transfers the contents of rdr to wrtr.
// if after == 0 then issues wrtr.Insert when it is done
// if after > 0 then issues wrtr.Insert every 'after' lines (useful for sql Writers to prevent memory issues)
// if after < 0, then the output isn't Inserted (for testing)
// if ignore == true, the read errors are ignored
func Export(rdr Input, wrtr Output, after int, ignore bool) error {
	var data []Row
	fds := rdr.TableSpec().FieldDefs
	sep := string(wrtr.Separator())

	for r := 0; ; r++ {
		var err error
		if data, _, err = rdr.Read(1, true); err != nil {
			// no need to report EOF
			if err == io.EOF {
				if after >= 0 {
					if e := wrtr.Insert(); e != nil {
						return e
					}
				}
				return nil
			}
			if ignore {
				continue
			}
			return Wrapper(ErrInput, fmt.Sprintf("%v", r))
		}

		// with the row, create line which is a []byte array of the fields separated by wrtr.Separator()
		line := make([]byte, 0)

		for c := 0; c < len(data[0]); c++ {
			// if we don't keep this field, move on to next one
			if fds[c].Drop {
				continue
			}
			var l interface{}

			if s, ok := l.(string); ok {
				if strings.Contains(s, wrtr.Text()) {
					l = strings.ReplaceAll(s, wrtr.Text(), wrtr.Text()+wrtr.Text())
				}
			}

			if reflect.ValueOf(data[0][c]).Kind() == reflect.Slice {
				line = append(line, WriteArray(data[0][c], sep, wrtr.Text())...)
			} else {
				line = append(line, WriteElement(data[0][c], sep, wrtr.Text())...)
			}
		}
		// replace last comma
		char := byte(' ')
		if wrtr.EOL() != 0 {
			char = byte(wrtr.EOL())
		}

		line[len(line)-1] = char
		// Now put the line to wrtr
		if _, err = wrtr.Write(line); err != nil {
			return Wrapper(ErrInput, fmt.Sprintf("%v", r))
		}

		if r > 0 && after > 0 && r%after == 0 {
			if e := wrtr.Insert(); e != nil {
				return e
			}
		}
	}
}

// Concur executes Export concurrently on a slice of Inputs/Outputs.
func Concur(nWorker int, rdrs []Input, wrtrs []Output, after int) error {
	queueLen := len(rdrs)
	defer func() {
		for ind := 0; ind < queueLen; ind++ {
			_ = rdrs[ind].Close()
			_ = wrtrs[ind].Close()
		}
	}()
	if nWorker == 0 {
		nWorker = runtime.NumCPU()
	}
	if len(rdrs) != len(wrtrs) {
		return Wrapper(ErrRWNum, fmt.Sprintf("%v %v", len(rdrs), len(wrtrs)))
	}
	if nWorker > queueLen {
		nWorker = queueLen
	}
	c := make(chan error)

	running := 0
	for ind := 0; ind < queueLen; ind++ {
		ind := ind // Required since the function below is a closure
		go func() {
			c <- Export(rdrs[ind], wrtrs[ind], after, false)
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
	return nil
}

// CommentColumn adds a comment to column 'column' of table 'table'
func CommentColumn(table, column, comment string, conn *Connect) error {
	qry := fmt.Sprintf("ALTER TABLE %s comment column %s '%s'", table, column, comment)
	return conn.Execute(qry)
}

// CommentFds comments all the columns of table that are represented in fds.  Errors are ignored.
func CommentFds(table string, fds map[int]*FieldDef, conn *Connect) {
	for _, fd := range fds {
		_ = CommentColumn(table, fd.Name, fd.Description, conn)
	}
}

// GetComments returns a map of field comments from the slice of tables. The key is the field name and
// the value is the comment
func GetComments(conn *Connect, table ...string) (map[string]string, error) {
	descMap := make(map[string]string)

	for _, dbTable := range table {
		var (
			rows *sql.Rows
			err  error
		)

		sep := strings.Split(dbTable, ".")
		if len(sep) != 2 {
			return nil, fmt.Errorf("need db.table format in %s", table)
		}

		qry := fmt.Sprintf("select name, comment from system.columns where table = '%s' AND database = '%s'", sep[1], sep[0])
		var name, desc string

		if rows, err = conn.Query(qry); err != nil {
			return nil, err
		}

		for rows.Next() {
			if e := rows.Scan(&name, &desc); e != nil {
				return nil, e
			}
			descMap[name] = desc
		}
	}

	return descMap, nil
}

// GetComment returns the comment for "field" from db.table
func GetComment(table, field string, conn *Connect) (comment string, err error) {
	var rows *sql.Rows

	sep := strings.Split(table, ".")
	if len(sep) != 2 {
		return "", fmt.Errorf("need db.table format in %s", table)
	}

	qry := fmt.Sprintf("select comment from system.columns where table = '%s' AND database = '%s' AND name='%s'", sep[1], sep[0], field)

	if rows, err = conn.Query(qry); err != nil {
		return "", err
	}

	rows.Next()
	if e := rows.Scan(&comment); e != nil {
		return "", e
	}

	return comment, nil
}
