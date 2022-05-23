package chutils

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

type mockRead struct {
	s         [][]string
	RowsRead  int
	name      string
	tableSpec *TableDef
}

func (m *mockRead) TableSpec() *TableDef {
	return m.tableSpec
}

func (m *mockRead) Read(nTarget int, validate bool) (data []Row, valid []Valid, err error) {
	err = nil
	data = nil
	valid = nil
	_ = validate // validate tested separately
	if m.RowsRead < len(m.s) {
		for r := 0; r < nTarget; r++ {
			d := make(Row, 0)
			for ind := 0; ind < len(m.s[m.RowsRead]); ind++ {
				d = append(d, m.s[m.RowsRead][ind])
			}
			data = append(data, d)
			m.RowsRead++
		}
		return
	}
	return nil, nil, io.EOF
}

func (m *mockRead) Reset() error {
	m.RowsRead = 0
	return nil
}

func (m *mockRead) CountLines() (numLines int, err error) {
	m.RowsRead = 0
	return len(m.s), nil
}

func (m *mockRead) Seek(lineNo int) error {
	if lineNo > len(m.s) {
		return io.EOF
	}
	m.RowsRead = lineNo
	return nil
}

func (m *mockRead) Close() error {
	return nil
}

func buildTableDef() TableDef {
	td := TableDef{
		Key:       "xstr",
		Engine:    MergeTree,
		FieldDefs: nil,
	}

	fd := make(map[int]*FieldDef)
	fnames := []string{"xstr", "xint", "xflt", "xdt"}
	for ind := 0; ind < len(fnames); ind++ {
		fdx := &FieldDef{
			Name:        fnames[ind],
			ChSpec:      ChField{},
			Description: "",
			Legal:       NewLegalValues(),
			Missing:     nil,
			Width:       0,
		}
		fd[ind] = fdx
	}
	td.FieldDefs = fd
	return td

}

func TestTableDef_Impute(t *testing.T) {
	data := [][]string{{"a", "1", "2.2", "1/2/2020"},
		{"c", "2", "3.3", "3/1/2002"},
		{"d", "3", "4.4", "12/31/1999"},
		{"f", "4", "5.5", "6/7/2010"}}
	rdr := mockRead{
		s:        data,
		RowsRead: 0,
		name:     "test",
	}
	td := buildTableDef()
	if e := td.Impute(&rdr, 0, .95); e != nil {
		t.Errorf("unexpected Impute error")
	}
	expected := []ChType{ChString, ChInt, ChFloat, ChDate}
	for ind, fd := range td.FieldDefs {
		if fd.ChSpec.Base != expected[ind] {
			t.Errorf("expected type %v got type %v", expected[ind], fd.ChSpec.Base)
		}
	}
}

func TestFieldDef_Validator(t *testing.T) {
	td := buildTableDef()
	types := []ChType{ChInt, ChFloat, ChString, ChDate, ChFixedString}
	highs := []interface{}{int64(3), 5.5, "", ""}
	lows := []interface{}{int64(0), 1.0, "", ""}
	inputs := [][]interface{}{{"1", "a", "1.4", "111"},
		{"1.1", "a", "-10.0"},
		{"hello", "abc"},
		{"1/2/2020", "2020/06/07", "1/2/2006", "1999/01/01"},
		{"A", "CC"},
	}
	expected := [][]Status{
		{VPass, VTypeFail, VTypeFail, VValueFail},
		{VPass, VTypeFail, VValueFail},
		{VValueFail, VPass},
		{VTypeFail, VPass, VTypeFail, VValueFail},
		{VPass, VTypeFail},
	}
	fd := td.FieldDefs[0]
	for r, ty := range types {
		fd.ChSpec.Base = ty
		switch ty {
		case ChInt, ChFloat:
			fd.Legal.HighLimit, fd.Legal.LowLimit, fd.ChSpec.Length = highs[r], lows[r], 64
		case ChFixedString:
			fd.ChSpec.Length = 1
			fd.Legal.Levels = nil
		case ChString:
			levels := []string{"abc", "def"}
			fd.Legal.Levels = levels
		case ChDate:
			fd.ChSpec.Format = "2006/01/02"
			dt := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			fd.Legal.LowLimit = dt
			dt1 := time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)
			fd.Legal.HighLimit = dt1
		}
		for c := 0; c < len(inputs[r]); c++ {
			_, status := fd.Validator(inputs[r][c])
			if status != expected[r][c] {
				fmt.Println(c, inputs[r][c])
				t.Errorf("expected %v got %v", expected[r][c], status)
			}
		}
	}
}

func TestTableDef_Check(t *testing.T) {
	fd1 := NewFieldDef("a", ChField{
		Base:   ChString,
		Length: 0,
		Funcs:  nil,
		Format: "",
	},
		"", NewLegalValues(), "", 0)
	fd2 := NewFieldDef("b", ChField{
		Base:   ChString,
		Length: 0,
		Funcs:  nil,
		Format: "",
	},
		"", NewLegalValues(), "", 0)
	fds := make(map[int]*FieldDef)
	fds[0], fds[1] = fd1, fd2
	td := NewTableDef("", MergeTree, fds)

	keys := []string{"a", "a, b", "a, b, c"}
	expected := []interface{}{nil, nil, ErrFields}
	for j, k := range keys {
		td.Key = k
		e := td.Check()
		if x := errors.Unwrap(e); x != expected[j] {
			t.Errorf("expected %v got %v", expected[j], x)
		}
	}
}

func TestTableDef_Nest(t *testing.T) {
	//	ch, err := NewChField(ChString, 0, OuterFuncs{OuterArray}, "")
	ch := ChField{
		Base:   ChString,
		Length: 0,
		Funcs:  OuterFuncs{OuterArray},
		Format: "",
	}
	fds := make(map[int]*FieldDef)
	fd1 := NewFieldDef("f1", ch, "", NewLegalValues(), nil, 0)
	fd2 := NewFieldDef("f2", ch, "", NewLegalValues(), nil, 0)
	fd3 := NewFieldDef("f3", ch, "", NewLegalValues(), nil, 0)
	fd4 := NewFieldDef("f4", ch, "", NewLegalValues(), nil, 0)
	fds[0], fds[1], fds[2], fds[3] = fd1, fd2, fd3, fd4
	td := TableDef{
		Key:       "f1",
		Engine:    MergeTree,
		FieldDefs: fds,
		nested:    nil,
	}
	if td.Nest("test", "f1", "f1") == nil {
		t.Errorf("error in test 0")
	}
	if td.Nest("test", "f1", "f2") != nil {
		t.Errorf("error in test 1")
	}
	if td.Nest("test", "f1", "f2") == nil {
		t.Errorf("error in test 2")
	}
	if td.Nest("test1", "f2", "f3") == nil {
		t.Errorf("error in test3")
	}
	if td.Nest("test1", "f3", "f4") != nil {
		t.Errorf("error in test4")
	}
}

func TestChType_String(t *testing.T) {
	expect := []string{"LowCardinality(Nullable(FixedString(1)))", "LowCardinality(Nullable(Int64))",
		"Array(LowCardinality(Nullable(Int64)))"}
	ch1 := ChField{
		Base:   ChFixedString,
		Length: 1,
		Funcs:  OuterFuncs{OuterNullable, OuterLowCardinality},
		Format: "",
	}
	ch2 := ChField{
		Base:   ChInt,
		Length: 64,
		Funcs:  OuterFuncs{OuterNullable, OuterLowCardinality},
		Format: "",
	}
	ch3 := ChField{
		Base:   ChInt,
		Length: 64,
		Funcs:  OuterFuncs{OuterNullable, OuterLowCardinality, OuterArray},
		Format: "",
	}
	chs := []ChField{ch1, ch2, ch3}
	for j, ch := range chs {
		v := fmt.Sprintf("%v", ch)
		assert.Equal(t, expect[j], v)
	}
}
