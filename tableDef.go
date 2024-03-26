package chutils

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/invertedv/keyval"
)

/*
The code in this file enables the creation of a TableDef and its components from the files in a directory
where each file defines one field.  The directory can either be in the OS or embedded in Go.

The field name is taken as the file name. Each file consists of key/val pairs (separated by a :). The key/vals
define the field.
*/
const (
	// legal values of keys
	fieldName        = "fieldName"
	fieldType        = "type"
	fieldOrder       = "order"
	fieldLength      = "length"
	fieldLevels      = "levels"
	fieldHigh        = "high"
	fieldLow         = "low"
	fieldMissing     = "missing"
	fieldDefault     = "default"
	fieldDescription = "description"
	fieldKey         = "key"
	fieldFormat      = "format"

	// default lengths of ints/floats
	defaultLenInt   = 32
	defaultLenFloat = 64

	// type names as strings
	typeString      = "string"
	typeFixedString = "fixedString"
	typeInt         = "int"
	typeFloat       = "float"
	typeDate        = "date"
)

var (
	requiredFields = []string{fieldOrder, fieldType, fieldOrder}
	allFields      = []string{fieldName, fieldType, fieldOrder, fieldLength, fieldLevels, fieldHigh, fieldLow, fieldMissing,
		fieldDefault, fieldDescription, fieldKey, fieldFormat}
)

// NewTableDefKV creates a *TableDef from a directory of files.  Each file defines a field. The file consists of key/val
// pairs which define the field.  The list of keys is below.  The field name is the name of the file.
//
// key - field name
// string is in keyval format.
// Valid Keys are:
//
//   - type*.  float, int, string, fixedString, date
//
//   - order*. order of the field in the table, starting with 0.
//
//   - key. Order of key: 0, 1, 2,....
//
//   - length.  Appropriate length for the field type, if applicable.  Defaults to 32 for int, 64  for float.
//     Required for fixed string.
//
//   - low. Minimum legal value (float, int, date).
//
//   - high. Maximum legal value (float, int, date).
//
//   - levels. Legal levels (string, fixedString)
//
//   - missing*.  Value to use if the actual value is illegal.
//
//   - default*. Value to use if the actual value is missing.
//
//   - format. Date format for date fields.
//
//   - description. Description of the field.
//
//     *required.
func NewTableDefKV(defs map[string]string) (td *TableDef, err error) {
	const sep = ":"

	keys := make(map[int]string)

	fds := make(map[int]*FieldDef)
	for k, v := range defs {
		var kv keyval.KeyVal

		if kv, err = StrToCol(v, sep, false); err != nil {
			return nil, err
		}

		kv["fieldName"] = &keyval.Value{AsString: k}

		if e := checkKeys(kv); e != nil {
			return nil, e
		}

		var fd *FieldDef
		if fd, err = NewFieldDefKV(kv); err != nil {
			return nil, err
		}

		fds[*kv.Get(fieldOrder).AsInt] = fd

		if kv.Present(fieldKey) != nil {
			var ord *int
			if ord = kv.Get(fieldKey).AsInt; ord == nil {
				return nil, fmt.Errorf("field %s: invalid value for key", kv.GetTrim(fieldName))
			}

			keys[*ord] = kv.GetTrim(fieldName)
		}
	}

	if e := checkOrder(fds); e != nil {
		return nil, e
	}

	var key string
	if key, err = makeKeys(keys); err != nil {
		return td, err
	}

	td = NewTableDef(key, MergeTree, fds)

	return td, nil
}

// makeKeys creates a key field for ClickHouse. The input keys map has key equal to the field name
// and the value is its order in the key
func makeKeys(keys map[int]string) (key string, err error) {
	keySlc := []string{}
	for ind := 0; ind < len(keys); ind++ {
		val, ok := keys[ind]

		if !ok {
			return "", fmt.Errorf("no key order value for %d", ind)
		}

		keySlc = append(keySlc, val)
	}

	return strings.Join(keySlc, ","), nil
}

// checkOrder checks that the order key increments by 1 starting with 0.  The fields are placed in this
// order in the table.
func checkOrder(fds map[int]*FieldDef) error {
	var ord []int

	for k := range fds {
		ord = append(ord, k)
	}

	sort.Ints(ord)

	if ord[0] != 0 {
		return fmt.Errorf("first order value must be 0, got %d", ord[0])
	}

	for ind := 1; ind < len(ord); ind++ {
		if ord[ind]-ord[ind-1] > 1 {
			return fmt.Errorf("order gap: have %d to %d", ord[ind], ord[ind-1])
		}
	}

	return nil
}

// parseNumeric parse the value in kv[field] to numeric according to the fieldType
func parseNumeric(field string, kv keyval.KeyVal) any {
	if kv.Present(field) == nil {
		return nil
	}

	l, val := kv.Get(fieldLength).AsInt, kv.Get(field)

	switch kv.GetTrim(fieldType) {
	case typeFloat:
		if *l == 32 {
			return float32(*val.AsFloat)
		}

		return float64(*val.AsFloat)

	case typeInt:
		if *l == 32 {
			return int32(*val.AsInt)
		}

		return int64(*val.AsInt)

	case typeDate:
		return *val.AsDate
	}

	return nil
}

// NewLegalKV creates a new *LegalValues based on the entries of kv
func NewLegalKV(kv keyval.KeyVal) *LegalValues {
	if kv.Present(strings.Join([]string{fieldLow, fieldHigh, fieldLevels}, ",")) == nil {
		return nil
	}

	lgl := &LegalValues{}

	if kv.Present(fieldHigh) != nil {
		lgl.HighLimit = parseNumeric(fieldHigh, kv)
	}

	if kv.Present(fieldLow) != nil {
		lgl.LowLimit = parseNumeric(fieldLow, kv)
	}

	if kv.Present(fieldLevels) != nil {
		lgl.Levels = kv.Get(fieldLevels).AsSliceS
	}

	return lgl
}

// getVal gets the (correctly typed) value of field from kv
func getVal(field string, kv keyval.KeyVal) any {
	if kv.Present(field) == nil {
		return nil
	}

	switch kv.GetTrim(fieldType) {
	case typeFloat, typeInt, typeDate:
		return parseNumeric(field, kv)
	case typeString, typeFixedString:
		return kv.GetTrim(field)
	}

	return nil
}

// NewCHFieldKV creates a new ChField based on the entries of kv
func NewChFieldKV(kv keyval.KeyVal) ChField {
	var (
		base ChType
		l    *int
	)

	l = new(int)
	*l = 0

	switch kv.GetTrim(fieldType) {
	case typeInt:
		base = ChInt
		l = kv.Get(fieldLength).AsInt
	case typeFloat:
		base = ChFloat
		l = kv.Get(fieldLength).AsInt
	case typeDate:
		base = ChDate
	case typeString:
		base = ChString
	case typeFixedString:
		base = ChFixedString
		l = kv.Get(fieldLength).AsInt
	}

	return ChField{
		Base:   base,
		Length: *l,
		Funcs:  nil,
		Format: kv.GetTrim(fieldFormat),
	}
}

// NewFieldDefKV builds a FieldDef based on the entries of kv
func NewFieldDefKV(kv keyval.KeyVal) (fd *FieldDef, err error) {
	lgl := NewLegalKV(kv)

	fd = &FieldDef{
		Name:        kv.GetTrim(fieldName),
		ChSpec:      NewChFieldKV(kv),
		Description: kv.GetTrim(fieldDescription),
		Legal:       lgl,
		Missing:     getVal(fieldMissing, kv),
		Default:     getVal(fieldDefault, kv),
		Width:       0,
		Drop:        false,
	}

	return fd, nil
}

// checkKeys checks that all the keys in kv are internally consistent
func checkKeys(kv keyval.KeyVal) error {
	if miss := kv.Missing(strings.Join(requiredFields, ",")); miss != nil {
		return fmt.Errorf("missing %v", miss)
	}

	fn := kv.GetTrim(fieldName)
	ft := kv.GetTrim(fieldType)

	if extra := kv.Unknown(strings.Join(allFields, ",")); extra != nil {
		return fmt.Errorf("field %s: unknown keys %v", fn, extra)
	}

	switch ft {
	case typeInt, typeFloat:
		return checkNumeric(kv)
	case typeString:
		return checkString(kv)
	case typeFixedString:
		return checkFixedString(kv)
	case typeDate:
		return checkDate(kv)
	default:
		return fmt.Errorf("%s has unknown type %s", fn, ft)
	}
}

// checkDate checks the entries in kv for a date field make sense
func checkDate(kv keyval.KeyVal) error {
	fn := kv.GetTrim(fieldName)

	if kv.Present(fieldLength) != nil {
		return fmt.Errorf("%s: date does not admit length key", fn)
	}

	if kv.Present(fieldLevels) != nil {
		return fmt.Errorf("field %s: cannot have levels", fn)
	}

	fm := kv.GetTrim(fieldFormat)
	if fm == "" {
		return nil
	}

	for _, okfmt := range DateFormats {
		if fm == okfmt {
			return nil
		}
	}

	return fmt.Errorf("field %s: unknown format %s", fn, fm)
}

// checkString checks the entries in kv for a string field make sense
func checkString(kv keyval.KeyVal) error {
	fn := kv.GetTrim(fieldName)

	if kv.Present(fieldFormat) != nil {
		return fmt.Errorf("field %s: strings do not admit formats", fn)
	}

	if kv.Present(fmt.Sprintf("%s,%s", fieldHigh, fieldLow)) != nil {
		return fmt.Errorf("field %s: cannot have high/low", fn)
	}

	if l := kv.Present(fieldLength); l != nil {
		return fmt.Errorf("%s: string does not admit length key, use fixedString", fn)
	}

	return nil
}

// checkFixedString checks the entries in kv for a fixedString field make sense
func checkFixedString(kv keyval.KeyVal) error {
	fn := kv.GetTrim(fieldName)

	if kv.Present(fieldFormat) != nil {
		return fmt.Errorf("field %s: fixedStrings do not admit formats", fn)
	}

	if kv.Present(fmt.Sprintf("%s,%s", fieldHigh, fieldLow)) != nil {
		return fmt.Errorf("field %s: cannot have high/low", fn)
	}

	if kv.Present(fieldLength) == nil {
		return fmt.Errorf("%s: fixedString requires %s key", fn, fieldLength)
	}

	if l := kv.Get(fieldLength).AsInt; *l < 1 {
		return fmt.Errorf("%s: illegal length for fixedString", fn)
	}

	return nil
}

// checkNumeric checks the entries in kv for a int/float field make sense
func checkNumeric(kv keyval.KeyVal) error {
	fn := kv.GetTrim(fieldName)
	ft := kv.GetTrim(fieldType)

	if kv.Present(fieldFormat) != nil {
		return fmt.Errorf("field %s: ints/floats do not admit formats", fn)
	}

	if kv.Present(fieldLevels) != nil {
		return fmt.Errorf("field %s: cannot have levels", fn)
	}

	if kv.Present(fieldLength) != nil {
		if l := kv.GetTrim(fieldLength); l != "32" && l != "64" {
			return fmt.Errorf("field %s: illegal length for type %s at %s", fn, ft, l)
		}
	}

	switch ft {
	case typeInt:
		def := int(defaultLenInt)
		kv[fieldLength] = &keyval.Value{AsInt: &def}
	case typeFloat:
		def := int(defaultLenFloat)
		kv[fieldLength] = &keyval.Value{AsInt: &def}
	}

	return nil
}

// StrToCol takes a string that's in key/val format and returns a Keyval object
func StrToCol(inStr, sep string, skipFirst bool) (keyval.KeyVal, error) {
	pairs := strings.Split(inStr, "\n")
	var k, v []string

	for ind := 0; ind < len(pairs); ind++ {
		if ind == 0 && skipFirst {
			continue
		}

		if strings.ReplaceAll(pairs[ind], " ", "") == "" {
			continue
		}

		row := strings.Split(pairs[ind], sep)
		// This allows for multi-line values
		if strings.Contains(row[0], "//") {
			continue
		}

		if len(row) == 1 {
			v[len(v)-1] = fmt.Sprintf("%s\n%s", v[len(v)-1], row[0])
			continue
		}

		if len(row) > 2 {
			return nil, fmt.Errorf("bad row strToCol: %s", pairs[ind])
		}

		k = append(k, strings.Trim(row[0], "\n"))
		v = append(v, strings.Trim(row[1], "\n"))
	}

	kv, e := keyval.ProcessKVs(k, v)
	if e != nil {
		return nil, e
	}

	return kv, nil
}

// ReadFS reads the all the files in embedded directory defDir and returns the results as a map. The key to the map is the
// file name, the value is the contents of the file.
func ReadFS(defDir embed.FS, startDir string) (defs map[string]string, err error) {
	var entries []fs.DirEntry

	if entries, err = defDir.ReadDir(startDir); err != nil {
		return nil, err
	}

	defs = make(map[string]string)
	for _, entry := range entries {
		name := fmt.Sprintf("%s/%s", startDir, entry.Name())
		if startDir == "." {
			name = entry.Name()
		}

		if entry.IsDir() {
			var defsSub map[string]string

			if defsSub, err = ReadFS(defDir, name); err != nil {
				return nil, err
			}

			for k, v := range defsSub {
				defs[k] = v
			}

			continue
		}

		var defn []byte
		if defn, err = defDir.ReadFile(name); err != nil {
			return nil, err
		}

		defs[entry.Name()] = string(defn)
	}

	return defs, nil
}

// ReadOS reads the all the files indirectory dir and returns the results as a map. The key to the map is the
// file name, the value is the contents of the file.
func ReadOS(dir string) (defs map[string]string, err error) {
	var entries []os.DirEntry

	if entries, err = os.ReadDir(dir); err != nil {
		return nil, err
	}

	defs = make(map[string]string)
	for _, entry := range entries {
		name := fmt.Sprintf("%s/%s", dir, entry.Name())

		if entry.IsDir() {
			var defsSub map[string]string

			if defsSub, err = ReadOS(name); err != nil {
				return nil, err
			}

			for k, v := range defsSub {
				defs[k] = v
			}

			continue
		}

		var defn []byte
		if defn, err = os.ReadFile(name); err != nil {
			return nil, err
		}

		defs[entry.Name()] = string(defn)
	}

	return defs, nil
}
