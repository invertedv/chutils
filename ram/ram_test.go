package ram

import (
	"testing"

	"github.com/invertedv/chutils"
	"github.com/stretchr/testify/assert"
)

func TestAnyData_Read(t *testing.T) {
	var (
		rdr *Reader
		err error
		r   []chutils.Row
		v   []chutils.Valid
	)
	rows := []chutils.Row{
		{1, "a"},
		{2, "b"},
		{3, "c"},
	}

	fdX := &chutils.FieldDef{
		Name:        "x",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: int32(0), HighLimit: int32(2)},
		Missing:     int32(10),
		Default:     nil,
		Width:       0,
		Drop:        false,
	}

	fdY := &chutils.FieldDef{
		Name:        "",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "",
		Legal:       &chutils.LegalValues{},
		Missing:     nil,
		Default:     nil,
		Width:       0,
		Drop:        false,
	}

	fds := make(map[int]*chutils.FieldDef)
	fds[0], fds[1] = fdX, fdY
	tabSpec := chutils.NewTableDef("x", chutils.MergeTree, fds)

	rdr, err = NewReader(rows, tabSpec)
	assert.Nil(t, err)

	r, _, err = rdr.Read(1, false)
	assert.Nil(t, err)
	assert.ElementsMatch(t, r[0], rows[0])

	err = rdr.Seek(3)
	assert.Nil(t, err)

	r, v, err = rdr.Read(1, true)
	assert.Nil(t, err)
	assert.ElementsMatch(t, r[0], chutils.Row{int32(10), "c"})
	assert.ElementsMatch(t, v[0], chutils.Valid{chutils.VValueFail, chutils.VPass})

	err = rdr.Reset()
	assert.Nil(t, err)

	r, _, err = rdr.Read(1, true)
	assert.Nil(t, err)
	assert.ElementsMatch(t, chutils.Row{int32(1), "a"}, r[0])
}
