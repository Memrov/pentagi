package adk

import (
	"context"
	"iter"
	"sync"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/googleadk"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/adk/v2/model"
)

// fakeSandbox records every argv it is asked to run and answers from a queue;
// a missing or nil next function returns the zero result.
type fakeSandbox struct {
	mu     sync.Mutex
	argv   [][]string
	next   func(argv []string) (ExecResult, error)
	called chan struct{}
}

func (f *fakeSandbox) Exec(ctx context.Context, argv []string, _ ExecOptions) (ExecResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}
	f.mu.Lock()
	f.argv = append(f.argv, append([]string(nil), argv...))
	next := f.next
	f.mu.Unlock()
	if f.called != nil {
		select {
		case f.called <- struct{}{}:
		default:
		}
	}
	if next == nil {
		return ExecResult{}, nil
	}
	return next(argv)
}

func (f *fakeSandbox) calls() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.argv...)
}

// testRegistry adapts the test environment to worker.Registry; Nexus service
// registration is the only method the environment lacks.
type testRegistry struct {
	*testsuite.TestWorkflowEnvironment
}

func (testRegistry) RegisterNexusService(*nexus.Service) {}

// recordingModel wraps a FakeModel and captures each request — FakeModel
// discards them, but the prompt seam can only be proven by what the model
// was sent.
type recordingModel struct {
	inner *googleadk.FakeModel
	mu    sync.Mutex
	reqs  []*model.LLMRequest
}

func newRecordingModel(name string, responses ...*model.LLMResponse) *recordingModel {
	return &recordingModel{inner: googleadk.NewFakeModel(responses...).WithName(name)}
}

func (r *recordingModel) Name() string { return r.inner.Name() }

func (r *recordingModel) GenerateContent(ctx context.Context, req *model.LLMRequest, stream bool) iter.Seq2[*model.LLMResponse, error] {
	r.mu.Lock()
	r.reqs = append(r.reqs, req)
	r.mu.Unlock()
	return r.inner.GenerateContent(ctx, req, stream)
}

func (r *recordingModel) requests() []*model.LLMRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*model.LLMRequest(nil), r.reqs...)
}
