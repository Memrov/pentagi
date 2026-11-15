package adk

import (
	"context"
	"sync"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/testsuite"
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
