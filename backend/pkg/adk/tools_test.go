package adk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/genai"
)

type declarer interface {
	Declaration() *genai.FunctionDeclaration
}

// schemaJSON normalizes the two declaration shapes: ActivityAsTool fills
// Parameters, functiontool fills ParametersJsonSchema.
func schemaJSON(t *testing.T, tl tool.Tool) map[string]any {
	t.Helper()
	d, ok := tl.(declarer)
	require.True(t, ok, "tool %q has no function declaration", tl.Name())
	decl := d.Declaration()
	schema := decl.Parameters
	if schema == nil {
		raw, err := json.Marshal(decl.ParametersJsonSchema)
		require.NoError(t, err)
		var m map[string]any
		require.NoError(t, json.Unmarshal(raw, &m))
		return m
	}
	raw, err := json.Marshal(schema)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

func schemaProps(t *testing.T, tl tool.Tool) map[string]any {
	t.Helper()
	props, _ := schemaJSON(t, tl)["properties"].(map[string]any)
	return props
}

func TestDurableTools_ExposesNamedTools(t *testing.T) {
	for _, tc := range []struct {
		name        string
		payloadRead bool
		want        []string
	}{
		{"without a store the reader stays unadvertised", false,
			[]string{"hack_result", "terminal", "file", "nuclei_scan", "httpx_probe"}},
		{"a configured store adds payload_read", true,
			[]string{"hack_result", "terminal", "file", "nuclei_scan", "httpx_probe", "payload_read"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tools, err := DurableTools(time.Minute, tc.payloadRead)
			require.NoError(t, err)
			require.Len(t, tools, len(tc.want))

			byName := map[string]bool{}
			for _, tl := range tools {
				byName[tl.Name()] = true
				assert.NotEmpty(t, tl.Description())
			}
			for _, name := range tc.want {
				assert.True(t, byName[name], "tool %q missing", name)
			}
		})
	}
}

// The schema names are the production tool contract: the prompt the model was
// trained against refers to these exact argument names.
func TestDurableTools_SchemaCarriesProductionFields(t *testing.T) {
	tools, err := DurableTools(time.Minute, true)
	require.NoError(t, err)

	for _, tl := range tools {
		props := schemaProps(t, tl)
		switch tl.Name() {
		case TerminalToolName:
			assert.Contains(t, props, "input")
			assert.Contains(t, props, "detach")
			assert.Contains(t, props, "timeout")
			assert.NotContains(t, props, "command")
		case FileToolName:
			assert.Contains(t, props, "action")
			assert.Contains(t, props, "path")
		case HackResultToolName:
			assert.Contains(t, props, "result")
			assert.Contains(t, props, "success")
		case NucleiToolName, HttpxToolName:
			assert.Contains(t, props, "target")
		case PayloadReadToolName:
			assert.Contains(t, props, "uri")
			assert.Contains(t, props, "offset_bytes")
			assert.Contains(t, props, "max_bytes")
		}
	}
}
