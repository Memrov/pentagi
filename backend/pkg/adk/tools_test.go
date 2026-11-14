package adk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genai"
)

type declarer interface {
	Declaration() *genai.FunctionDeclaration
}

func TestDurableTools_ExposesThreeNamedActivityTools(t *testing.T) {
	tools, err := DurableTools(time.Minute)
	require.NoError(t, err)
	require.Len(t, tools, 3)

	byName := map[string]bool{}
	for _, tl := range tools {
		byName[tl.Name()] = true
		assert.NotEmpty(t, tl.Description())
	}
	assert.True(t, byName["terminal"])
	assert.True(t, byName["nuclei_scan"])
	assert.True(t, byName["httpx_probe"])
}

func TestDurableTools_SchemaCarriesRequiredFields(t *testing.T) {
	tools, err := DurableTools(time.Minute)
	require.NoError(t, err)

	for _, tl := range tools {
		d, ok := tl.(declarer)
		require.True(t, ok, "tool %q has no function declaration", tl.Name())
		props := d.Declaration().Parameters.Properties
		switch tl.Name() {
		case TerminalToolName:
			assert.Contains(t, props, "command")
		case NucleiToolName, HttpxToolName:
			assert.Contains(t, props, "target")
		}
	}
}
