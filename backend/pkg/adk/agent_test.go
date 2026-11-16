package adk

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/googleadk"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/tool"
)

func TestNewPentesterAgent_BuildsWithAndWithoutGuard(t *testing.T) {
	mdl := googleadk.NewModel("fake-model")

	a, err := NewPentesterAgent(mdl, nil, "", nil)
	require.NoError(t, err)
	require.NotNil(t, a)
	require.Equal(t, "pentester", a.Name())

	guard := ScopeGuard(func(ctx agent.Context, tl tool.Tool, args map[string]any) (map[string]any, error) {
		return nil, nil
	})
	a, err = NewPentesterAgent(mdl, nil, "custom instruction", guard)
	require.NoError(t, err)
	require.NotNil(t, a)
}
