package adk

import (
	"strconv"
	"strings"
)

// FlowIDFromWorkflowID parses the flow id a FlowWorkflow child id carries:
// "<tenant->pentagi-flow-<flowID>-input-<seq>". The tenant prefix is opaque,
// so matching anchors on the last "-input-" run and the "pentagi-flow-"
// marker before it; anything outside that shape — a CLI spike run, a
// hand-typed id — returns false and the caller falls back to its configured
// sandbox.
func FlowIDFromWorkflowID(id string) (flowID int64, ok bool) {
	suffix := strings.LastIndex(id, "-input-")
	if suffix < 0 {
		return 0, false
	}
	if _, err := strconv.Atoi(id[suffix+len("-input-"):]); err != nil {
		return 0, false
	}
	base := id[:suffix]
	marker := strings.LastIndex(base, "pentagi-flow-")
	if marker < 0 {
		return 0, false
	}
	fid, err := strconv.ParseInt(base[marker+len("pentagi-flow-"):], 10, 64)
	if err != nil || fid <= 0 {
		return 0, false
	}
	return fid, true
}
