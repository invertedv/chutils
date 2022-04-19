package chutils

//TODO: add read and write functions to TableDefs -- JSON? yes? no?
//TODO implement outerFunc()

import (
	"database/sql"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"time"
)

type Reader struct {
	Skip      int       // Skip is the # of rows to skip in the file
	RowsRead  int       // Rowsread is current count of rows read from the file
	TableSpec *TableDef // TableDef is the table def for the file.  Can be supplied or derived from the file
}

type Loader struct {
	*Reader
}

// Input is the interface for reading source data (file, flat file, query)
type Input interface {
	Read(nTarget int, validate bool) (data []Row, err error) // Read reads rows from the source
	Reset()                                                  // Resets to beginning of table
	Close()                                                  // Closes file (for file-based sources)
	CountLines() (numLines int, err error)
	Seek(lineNo int) (err error)
}

type InputError struct {
	Err string
}

func (e *InputError) Error() string {
	return e.Err
}

type EngineType int

const (
	MergeTree EngineType = 0 + iota
	Memory
)

//go:generate stringer -type=EngineType

// ChType enum is supported ClickHouse types.
type ChType int

//ClickHouse base types supported
const (
	Unknown     ChType = 0 + iota // Unknown: ClickHouse type is undetermined
	Int                           // Int: ClickHouse type is Integer
	String                        // String: ClickHouse type is String
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
func (l *LegalValues) Update(newVal string, target *ChField) (res ChType) {
	// if target != Unknown, this will force the type indicated by target

	res = target.Base

	// Figure out what this newVal might be
	if res == Unknown {
		// order of assigning: Date, Int, Float, String
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
	Width       int                      // width of field (for flat files)
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

// TableDef defines a table
type TableDef struct {
	Name   string
	Key    string
	Engine EngineType
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

func FindFormat(inDate string) (format string, date time.Time, err error) {
	var formats = []string{"2006-01-02", "2006-1-2", "2006/01/02", "2006/1/2", "20060102", "01022006",
		"01/02/2006", "1/2/2006", "01-02-2006", "1-2-2006", "200601"}

	format = ""
	for _, format = range formats {
		date, err = time.Parse(format, inDate)
		if err == nil {
			return
		}
	}
	return
}

// TODO: change this to stream
// Impute looks at the data and builds the FieldDefs
func (td *TableDef) Impute(rdr Input, rowsToExamine int, tol float64, fuzz int) (err error) {
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
	// threshold to determine which type a field is (100*tol % agreement)
	thresh := int(math.Max(1.0, tol*float64(rowCount)))
	for ind := 0; ind < numFields; ind++ {
		fd := td.FieldDefs[ind]
		// only impute type if user has not specified it
		if fd.ChSpec.Base == Unknown {
			switch {
			case counts[ind].dates >= thresh:
				fd.ChSpec.Base, fd.ChSpec.Length = Date, 0
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
	qry = fmt.Sprintf("%s ENGINE=%v()\nORDER BY (%s)", qry, td.Engine, td.Key)
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

func Export(rdr Input, nTarget int, separator rune, outFile string) (err error) {
	const newLine = "\n"

	sep := string(separator)
	fmt.Println("start reading", time.Now())

	_ = os.Remove(outFile)
	f, err := os.Create(outFile)
	if err != nil {
		return
	}
	_ = f.Close()
	fmt.Println("start writing", time.Now())
	file, err := os.OpenFile(outFile, os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		return
	}
	var data []Row
	for r := 0; nTarget == 0 || (nTarget > 0 && r < nTarget); r++ {

		if data, err = rdr.Read(1, true); err != nil {
			// no need to report EOF
			if err == io.EOF {
				err = nil
			}
			if err != nil {
				return
			}
			fmt.Println("done writing", time.Now())
			return
		}
		for c := 0; c < len(data[0]); c++ {
			char := sep
			if c == len(data[0])-1 {
				char = newLine
			}
			switch v := data[0][c].(type) {
			case string:
				_, err = file.WriteString(fmt.Sprintf("'%s'%s", v, char))
			case time.Time:
				//a := fmt.Sprintf("%s%s", v.Format("2006-01-02"), string(char))
				//fmt.Println(a)
				_, err = file.WriteString(fmt.Sprintf("%s%s", v.Format("2006-01-02"), char))
			default:
				_, err = file.WriteString(fmt.Sprintf("%v%s", v, char))
			}
			if err != nil {
				return
			}
		}
	}
	fmt.Println("done writing", time.Now())
	return nil
}

type Connect struct {
	Host     string
	User     string
	Password string
}

// Load reads n lines from rdr and inserts them into table.
// tmpFile is a temporary file created to do bulk copy into ClickHouse
func Load(rdr Input, table string, n int, tmpFile string, con Connect) (err error) {
	err = Export(rdr, n, '|', tmpFile)
	if err != nil {
		return
	}
	err = InsertFile(table, tmpFile, '|', CSV, "", con.Host, con.User,
		con.Password)
	os.Remove(tmpFile)
	return
}

// Loads a ClickHouse table from an array of Inputs concurrently
func Concur(nChan int, rdrs []Input, table string, tmpRoot string, con Connect) (err error) {
	err = nil
	if nChan == 0 {
		nChan = runtime.NumCPU()
	}
	nReaders := len(rdrs)
	c := make(chan error, nChan)
	nObs, err := rdrs[0].CountLines()
	if err != nil {
		return
	}
	nper := int(nObs / nReaders)
	start := 1
	for j := 0; j < nReaders; j++ {
		if err = rdrs[j].Seek(start); err != nil {
			return
		}
		start += nper
	}
	for j := 0; j < nReaders; j++ {
		rand.Seed(time.Now().UnixMicro())
		tmpFile := fmt.Sprintf("%s/tmp%d.csv", tmpRoot, rand.Int31())
		num := nper
		if j == nReaders-1 {
			num = 0
		}
		j := j // Required since the function below is a closure
		go func() {
			c <- Load(rdrs[j], table, num, tmpFile, con)
			return
		}()
	}
	for j := 0; j < nReaders; j++ {
		e := <-c
		if e != nil {
			return e
		}
	}
	return
}
