package chutils

import (
	"embed"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	//go:embed testTD
	testDef embed.FS
)

func TestNewTableDefKV(t *testing.T) {
	defs, e := ReadFS(testDef, ".")
	if e != nil {
		fmt.Println(e)
	}
	assert.Nil(t, e)

	var td *TableDef
	td, e = NewTableDefKV(defs)
	fmt.Println(e)
	assert.Nil(t, e)

	user := os.Getenv("user")
	password := os.Getenv("password")
	conn, e := NewConnect("127.0.0.1", user, password, nil)
	assert.Nil(t, e)

	e = td.Create(conn, "tmp.test")
	assert.Nil(t, e)
}

