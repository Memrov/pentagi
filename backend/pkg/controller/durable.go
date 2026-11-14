package controller

import (
	"context"
	"fmt"

	"pentagi/pkg/temporal"
)

// FlowEngine drives flows through a durable orchestrator in place of the
// in-process worker goroutine: input is queued in the engine, which hands it
// back through the FlowRunner returned by NewDurableRunner.
type FlowEngine interface {
	StartFlow(ctx context.Context, flowID int64) error
	SignalInput(ctx context.Context, flowID int64, input string) error
	FinishFlow(ctx context.Context, flowID int64) error
}

type FlowControllerOption func(*flowController)

func WithFlowEngine(engine FlowEngine) FlowControllerOption {
	return func(fc *flowController) {
		fc.engine = engine
	}
}

// NewDurableRunner exposes the flows of fc to the durable engine's worker.
func NewDurableRunner(fc FlowController) (temporal.FlowRunner, error) {
	ctl, ok := fc.(*flowController)
	if !ok {
		return nil, fmt.Errorf("unsupported flow controller %T", fc)
	}
	return durableRunner{fc: ctl}, nil
}

type durableRunner struct {
	fc *flowController
}

func (r durableRunner) worker(ctx context.Context, flowID int64) (*flowWorker, error) {
	fw, err := r.fc.GetFlow(ctx, flowID)
	if err != nil || fw == nil {
		return nil, fmt.Errorf("flow %d: %w", flowID, temporal.ErrFlowNotLoaded)
	}
	w, ok := fw.(*flowWorker)
	if !ok {
		return nil, fmt.Errorf("flow %d has unsupported worker %T", flowID, fw)
	}
	return w, nil
}

func (r durableRunner) ResumeFlow(ctx context.Context, flowID int64) error {
	fw, err := r.worker(ctx, flowID)
	if err != nil {
		return err
	}
	return fw.durableResume(ctx)
}

func (r durableRunner) lastTaskID(ctx context.Context, flowID int64) (int64, error) {
	tasks, err := r.fc.db.GetFlowTasks(ctx, flowID)
	if err != nil {
		return 0, fmt.Errorf("failed to get flow %d tasks: %w", flowID, err)
	}
	var last int64
	for _, task := range tasks {
		last = max(last, task.ID)
	}
	return last, nil
}

func (r durableRunner) InputMark(ctx context.Context, flowID int64) (temporal.InputMark, error) {
	fw, err := r.worker(ctx, flowID)
	if err != nil {
		return temporal.InputMark{}, err
	}
	last, err := r.lastTaskID(ctx, flowID)
	if err != nil {
		return temporal.InputMark{}, err
	}
	mark := temporal.InputMark{LastTaskID: last}
	if task := fw.waitingTask(); task != nil {
		mark.WaitingTaskID = task.GetTaskID()
	}
	return mark, nil
}

func (r durableRunner) ProcessFlowInput(ctx context.Context, req temporal.InputRequest) error {
	fw, err := r.worker(ctx, req.FlowID)
	if err != nil {
		return err
	}
	if req.Recover {
		last, err := r.lastTaskID(ctx, req.FlowID)
		if err != nil {
			return err
		}
		return fw.durableRecover(ctx, req, last)
	}
	return fw.durableInput(ctx, req.Input)
}

// durableMX serializes runs of one flow: a retry of a timed-out activity
// waits for the run it replaces, which is left going rather than stopped,
// because a stopped task is persisted as Waiting and could no longer be told
// apart from one that asks the user.
func (fw *flowWorker) durableResume(ctx context.Context) error {
	fw.durableMX.Lock()
	defer fw.durableMX.Unlock()

	if fw.ctx.Err() != nil || !fw.resumeTasks() {
		return temporal.ErrFlowClosed
	}
	return nil
}

func (fw *flowWorker) durableInput(ctx context.Context, input string) error {
	fw.durableMX.Lock()
	defer fw.durableMX.Unlock()

	if fw.ctx.Err() != nil {
		return temporal.ErrFlowClosed
	}

	task, err := fw.processInput(flowInput{input: input, reply: newInputReply()})
	if !fw.settle(input, task, err, "failed to process input", "user input processed") {
		return temporal.ErrFlowClosed
	}
	return nil
}

// durableRecover finishes the work of an input whose earlier attempt was
// interrupted. A task newer than req.LastTaskID was created by that attempt
// and is resumed unless it already finished or waits for the user; one that
// did not load died before its subtasks were saved, and the input is
// processed afresh. The task that waited for the input is fed it again only
// while it still waits; a waiting task that took the input and asked the user
// again cannot be told apart and is fed twice. With neither, a task left
// running was fed the input and is resumed, or the attempt left no trace and
// the input is processed afresh.
func (fw *flowWorker) durableRecover(ctx context.Context, req temporal.InputRequest, lastTaskID int64) error {
	if lastTaskID > req.LastTaskID {
		task, err := fw.tc.GetTask(fw.ctx, lastTaskID)
		switch {
		case err != nil:
			return fw.durableInput(ctx, req.Input)
		case task.IsCompleted() || task.IsWaiting():
			return nil
		}
		return fw.durableContinue(ctx, task)
	}

	if req.WaitingTaskID != 0 {
		task, err := fw.tc.GetTask(fw.ctx, req.WaitingTaskID)
		switch {
		case err != nil || task.IsWaiting():
			return fw.durableInput(ctx, req.Input)
		case task.IsCompleted():
			return nil
		}
		return fw.durableContinue(ctx, task)
	}

	for _, task := range fw.tc.ListTasks(fw.ctx) {
		if !task.IsCompleted() && !task.IsWaiting() {
			return fw.durableContinue(ctx, task)
		}
	}

	return fw.durableInput(ctx, req.Input)
}

func (fw *flowWorker) durableContinue(ctx context.Context, task TaskWorker) error {
	fw.durableMX.Lock()
	defer fw.durableMX.Unlock()

	if fw.ctx.Err() != nil {
		return temporal.ErrFlowClosed
	}

	input := "continue after interruption"
	spanName := fmt.Sprintf("continue task %d: %s", task.GetTaskID(), task.GetTitle())
	err := fw.reportFailure(task, input, fw.runTask(spanName, input, task))
	if !fw.settle(input, task, err, "failed to continue task", "task continued successfully") {
		return temporal.ErrFlowClosed
	}
	return nil
}

func (fw *flowWorker) waitingTask() TaskWorker {
	for _, task := range fw.tc.ListTasks(fw.ctx) {
		if !task.IsCompleted() && task.IsWaiting() {
			return task
		}
	}
	return nil
}

func (fw *flowWorker) start(ctx context.Context) error {
	if fw.engine != nil {
		return fw.engine.StartFlow(ctx, fw.flowCtx.FlowID)
	}

	fw.wg.Add(1)
	go fw.worker()
	return nil
}
