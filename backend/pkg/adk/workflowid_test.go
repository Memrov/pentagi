package adk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlowIDFromWorkflowID_ParsesFlowChildIDs(t *testing.T) {
	cases := []struct {
		id   string
		want int64
		ok   bool
	}{
		{"pentagi-flow-7-input-0", 7, true},
		{"acme-pentagi-flow-42-input-3", 42, true},
		{"us-east-1-pentagi-flow-123456-input-12", 123456, true},
		{"pentagi-flow-7", 0, false},
		{"pentagi-flow-abc-input-1", 0, false},
		{"pentagi-flow-7-input-x", 0, false},
		{"pentagi-flow-0-input-1", 0, false},
		{"pentagi-flow-7-extra-input-3", 0, false},
		{"pentagi-flow-7-input-3-input-4", 0, false},
		{"pentagi-flow--input-1", 0, false},
		{"default-test-workflow-id", 0, false},
		{"adkpentest-cli-run", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			flowID, ok := FlowIDFromWorkflowID(tc.id)
			assert.Equal(t, tc.want, flowID)
			assert.Equal(t, tc.ok, ok)
		})
	}
}
