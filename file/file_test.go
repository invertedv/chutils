package file

import (
	"errors"
	"github.com/invertedv/chutils"
	"log"
	"strings"
	"testing"
	"time"
)

type rstr struct{ strings.Reader }

func (r *rstr) Close() error {
	return nil
}

func TestReader_Seek(t *testing.T) {
	input := []string{"a,b\n1,2\n3,4\n5,6\n7,8\n9,19\n"}
	seekto := []int{1, 2, 3, 1, 10}
	result := []string{"1", "3", "5", "1", "EOF"}

	rt := &rstr{*strings.NewReader(input[0])}
	rt1 := NewReader("abc", ',', '\n', 0, 0, 1, 0, rt, 0)
	if e := rt1.Init(); e != nil {
		t.Errorf("Init failed")
		return
	}
	for j := 0; j < len(result); j++ {
		e := rt1.Seek(seekto[j])
		if e != nil {
			if errors.Unwrap(e).(chutils.ErrType) != chutils.ErrSeek {
				t.Errorf("unexpected error seeking row %d on case %d", seekto[j], j)
			}
			if result[j] != "EOF" {
				t.Errorf("not expecting EOF on test %d", j)
			}
			break
		}

		l, _, e := rt1.Read(1, false)
		if l[0][0] != result[j] {
			t.Errorf("Results don't match on case %d. Expect %s but got %s", j, result[j], l[0][0])
		}

	}
}

func TestReader_Reset(t *testing.T) {
	readcnt := []int{1, 1, 2, 3}
	input := []string{"a,b\n1,2\n3,4\n5,6\n7,8\n9,19\n"}
	seekto := []int{1, 2, 3, 1}
	result := []string{"1", "1", "3", "5"}

	rt := &rstr{*strings.NewReader(input[0])}
	rt1 := NewReader("abc", ',', '\n', 0, 0, 1, 0, rt, 0)
	if e := rt1.Init(); e != nil {
		t.Errorf("Init failed")
		return
	}
	for j := 0; j < len(result); j++ {
		e := rt1.Seek(seekto[j])
		if e != nil {
			t.Errorf("unexpected error seeking row %d on case %d", seekto[j], j)
			break
		}

		if _, _, e := rt1.Read(readcnt[j], false); e != nil {
			t.Errorf("unexpected read error on case %d", j)
			break
		}

		rt1.Reset()
		r, _, e := rt1.Read(readcnt[j], false)
		if e != nil {
			t.Errorf("unexpected read error on case %d", j)
			break
		}
		if r[readcnt[j]-1][0] != result[j] {
			t.Errorf("expected %s but got %s", result[j], r[readcnt[j]-1][0])
		}
	}
}

func TestReader_Init(t *testing.T) {
	input := []string{"a,b\n1,2\n2,3\n", "a,hello\n1,2\n2,3\n", "a,b,c,d,e,f\n1,2,3,4,5,6\n"}
	results := [][]string{{"a", "b"}, {"a", "hello"}, {"a", "b", "c", "d", "e", "f"}}

	for j := 0; j < len(results); j++ {
		rt := &rstr{*strings.NewReader(input[j])}
		rt1 := NewReader("abc", ',', '\n', 0, 0, 1, 0, rt, 0)
		if e := rt1.Init(); e != nil {
			t.Errorf("unexpected error")
		}
		if len(rt1.TableSpec().FieldDefs) != len(results[j]) {
			t.Errorf("case %d expected %d fields got %d fields", j, len(results[j]), len(rt1.TableSpec().FieldDefs))
			break
		}
		for k := 0; k < len(results[j]); k++ {
			if results[j][k] != rt1.TableSpec().FieldDefs[k].Name {
				t.Errorf("case %d expected name %s got name %s", j, results[j][k], rt1.TableSpec().FieldDefs[j].Name)
			}
		}
	}
}

func TestReader_Read(t *testing.T) {
	input := []string{"a,b,c\n \"hello, mom\", 3, \"now, now\"\n,1,2,3\n",
		"a,b\n1,2\n2,3\n",
		"a,b\n1,2\n2,3\n",
		"a,b,c\n1,2\n"}
	row := []int{1, 1, 2, 1}
	col := []int{0, 0, 1, 0}
	quote := []rune{'"', '"', 0, '"'}
	result := []string{"hello, mom", "1", "3", "ErrFieldCount"}

	for j := 0; j < len(result); j++ {

		rt := &rstr{*strings.NewReader(input[j])}
		rt1 := NewReader("abc", ',', '\n', quote[j], 0, 1, 0, rt, 0)
		if e := rt1.Init(); e != nil {
			t.Errorf("unexpected Init error, case %d", j)
			break
		}
		rt.Seek(0, 0)
		if e := rt1.Seek(row[j]); e != nil {
			log.Fatalln(e)
		}
		d, _, e := rt1.Read(1, false)
		if e != nil {
			if errors.Unwrap(e).(chutils.ErrType) == chutils.ErrFieldCount {
				if result[j] == "ErrFieldCount" {
					break
				}
			}
			t.Errorf("unexpected read error case %d", j)
			break
		}
		if d[0][col[j]] != result[j] {
			t.Errorf("case %d element should be %v but got %v", j, result[j], d[0][0])
		}
	}

	// Test type conversions, HighLimit/LowLimit/levels legal values

	// data
	inputc := "a,b,c,d ,e,f,g,h,i,j\n 2000-01-03, 3.4,A  ,BD,42,2000/1/2,ABCD,10000.1,2030-01-01,Hi\n"
	// values for FieldDef
	field := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	base := []chutils.ChType{chutils.ChDate, chutils.ChFloat, chutils.ChFixedString, chutils.ChFixedString,
		chutils.ChInt, chutils.ChDate, chutils.ChFixedString, chutils.ChFloat, chutils.ChDate, chutils.ChString}
	length := []int{0, 64, 1, 2, 16, 0, 2, 64, 0, 0}
	datefmt := []string{"2006-01-02", "", "", "", "", "2006-01-02", "", "", "2006-01-02", ""}
	dtmiss := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	dtmax, _ := time.Parse("2006-01-02", "2022-12-31")
	missing := []interface{}{dtmiss, -1.0, "!", "X", -1, dtmiss, "!", -1.0, dtmiss, "0"}
	maxes := []interface{}{dtmax, 1000.0, "", "", 100, dtmax, "", 1000.0, dtmax}
	dtmin, _ := time.Parse("2006-01-02", "2000-01-01")
	mins := []interface{}{dtmin, 0.0, "", "", 0, dtmin, "", 0.0, dtmin}

	mp := make(map[string]int)
	mp["BD"] = 1
	mp1 := make(map[string]int)
	mp1["HI"] = 1
	levels := []map[string]int{nil, nil, nil, mp, nil, nil, nil, nil, nil, mp1}

	// test values
	col = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 9}
	dt, _ := time.Parse("2006-01-02", "2000-01-03")
	result1 := []interface{}{dt, 3.4, "A", "BD", 42, dtmiss, "!", -1.0, dtmiss, "X"}

	rt := &rstr{*strings.NewReader(inputc)}
	rt1 := NewReader("abc", ',', '\n', 0, 0, 1, 0, rt, 0)
	if e := rt1.Init(); e != nil {
		t.Errorf("unexpected Init error, case %d", 0)
	}
	for j := 0; j < len(field); j++ {
		_, fd, e := rt1.TableSpec().Get(field[j])
		if e != nil {
			t.Errorf("unexpected Get error, field: %s", field[j])
			return
		}
		fd.ChSpec.Base = base[j]
		fd.ChSpec.Length = length[j]
		fd.ChSpec.DateFormat = datefmt[j]
		fd.Missing = missing[j]
		if base[j] == chutils.ChInt || base[j] == chutils.ChFloat {
			fd.Legal.HighLimit = maxes[j]
			fd.Legal.LowLimit = mins[j]
		}
		if base[j] == chutils.ChString || base[j] == chutils.ChFixedString {
			fd.Legal.Levels = &levels[j]
		}
		if base[j] == chutils.ChDate {
			if dx, ok := maxes[j].(time.Time); ok {
				fd.Legal.LastDate = &dx
			}
			if dx, ok := mins[j].(time.Time); ok {
				fd.Legal.FirstDate = &dx
			}
		}
	}
	// read the row
	d, _, e := rt1.Read(1, true)
	if e != nil {
		t.Errorf("unexpected error reading type check case")
		return
	}
	// check output
	for j := 0; j < len(field); j++ {
		val := d[0][col[j]]
		switch base[j] {
		case chutils.ChFloat:
			if val.(float64) != result1[j].(float64) {
				t.Errorf("Type test, expected %v got %v", result1[j], val)
			}
		case chutils.ChDate:
			if val.(time.Time) != result1[j].(time.Time) {
				t.Errorf("Type test, expected %v got %v", result1[j], val)
			}
		case chutils.ChInt:
			if val.(int) != result1[j].(int) {
				t.Errorf("Type test, expected %v got %v", result1[j], val)
			}
		case chutils.ChFixedString:
			if val.(string) != result1[j].(string) {
				t.Errorf("Type test, expected %v got %v", result1[j], val)
			}
		}
	}
}

type wstr struct {
	buf string
}

func (w *wstr) Close() error {
	return nil
}
func (w *wstr) Write(b []byte) (n int, err error) {
	w.buf = w.buf + string(b)
	return len(b), nil
}

func (w *wstr) String() string {
	return string(w.buf)
}

func TestWriter_Export(t *testing.T) {
	var con chutils.Connect

	f := &wstr{""}
	wtr := NewWriter(f, "A", &con, ',', '\n', "table")

	input := []string{"a,b\n1,2\n3,4\n5,6\n7,8\n9,19\n"}

	result := "'1','2'\n'3','4'\n'5','6'\n'7','8'\n'9','19'\n"

	rt := &rstr{*strings.NewReader(input[0])}
	rt1 := NewReader("abc", ',', '\n', 0, 0, 1, 0, rt, 0)
	if e := rt1.Init(); e != nil {
		t.Errorf("Init failed")
		return
	}
	chutils.Export(rt1, wtr)
	if result != f.buf {
		t.Errorf("expected %s got %s", result, f.buf)
	}

	// once we specify the type of a as ChInt, the single quote goes away
	result = "1,'2'\n3,'4'\n5,'6'\n7,'8'\n9,'19'\n"
	rt1.Reset()
	fd := rt1.TableSpec().FieldDefs[0]
	fd.ChSpec.Base = chutils.ChInt
	fd.ChSpec.Length = 16
	f = &wstr{""}
	wtr = NewWriter(f, "A", &con, ',', '\n', "table")
	chutils.Export(rt1, wtr)
	if result != f.buf {
		t.Errorf("expected %s got %s", result, f.buf)
	}
}

func ExampleReader_Read() {

}
