// Code generated by "stringer -type=EngineType"; DO NOT EDIT.

package chutils

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[MergeTree-0]
	_ = x[Memory-1]
}

const _EngineType_name = "MergeTreeMemory"

var _EngineType_index = [...]uint8{0, 9, 15}

func (i EngineType) String() string {
	if i < 0 || i >= EngineType(len(_EngineType_index)-1) {
		return "EngineType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _EngineType_name[_EngineType_index[i]:_EngineType_index[i+1]]
}