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
	rt1 := NewReader("abc", ',', '\n', 0, 0, 1, 0, rt)
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

		l, e := rt1.Read(1, false)
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
	rt1 := NewReader("abc", ',', '\n', 0, 0, 1, 0, rt)
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

		if _, e := rt1.Read(readcnt[j], false); e != nil {
			t.Errorf("unexpected read error on case %d", j)
			break
		}

		rt1.Reset()
		r, e := rt1.Read(readcnt[j], false)
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
		rt1 := NewReader("abc", ',', '\n', 0, 0, 1, 0, rt)
		if e := rt1.Init(); e != nil {
			t.Errorf("unexpected error")
		}
		if len(rt1.TableSpec.FieldDefs) != len(results[j]) {
			t.Errorf("case %d expected %d fields got %d fields", j, len(results[j]), len(rt1.TableSpec.FieldDefs))
			break
		}
		for k := 0; k < len(results[j]); k++ {
			if results[j][k] != rt1.TableSpec.FieldDefs[k].Name {
				t.Errorf("case %d expected name %s got name %s", j, results[j][k], rt1.TableSpec.FieldDefs[j].Name)
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
		rt1 := NewReader("abc", ',', '\n', quote[j], 0, 1, 0, rt)
		if e := rt1.Init(); e != nil {
			t.Errorf("unexpected Init error, case %d", j)
			break
		}
		rt.Seek(0, 0)
		if e := rt1.Seek(row[j]); e != nil {
			log.Fatalln(e)
		}
		d, e := rt1.Read(1, false)
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
	rt := &rstr{*strings.NewReader(input[0])}
	rt1 := NewReader("abc", ',', '\n', quote[0], 0, 1, 0, rt)
	if e := rt1.Init(); e != nil {
		t.Errorf("unexpected Init error, case %d", 0)
	}

	// Loop on type conversions

	rt1.TableSpec.FieldDefs[1].ChSpec.Base = chutils.ChDate
	td := "2010-12-31"
	rt1.TableSpec.FieldDefs[1].Missing, _ = time.Parse("2006-01-02", td)

	rt1.TableSpec.FieldDefs[1].ChSpec.Base = chutils.ChInt
	rt1.TableSpec.FieldDefs[1].ChSpec.Length = 32

	d, e := rt1.Read(1, true)
	if e != nil {
		t.Errorf("unexpected error reading case 0 (2nd time")
		return
	}
	tm, ok := d[0][1].(time.Time)
	if !ok {
		t.Errorf("conversion error")
		return
	}
	if tm.Format("2006-01-02") != td {
		t.Errorf("error missing date")
	}

}
