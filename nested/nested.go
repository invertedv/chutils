package nested

import "github.com/invertedv/chutils"

type Reader struct {
	rdr chutils.Input
}

func NewReader(rdr chutils.Input) *Reader {
	return &Reader{rdr}
}
