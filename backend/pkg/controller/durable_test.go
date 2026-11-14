package controller

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pentagi/pkg/database"
	"pentagi/pkg/temporal"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingEngine struct {
	mx       sync.Mutex
	err      error
	started  []int64
	inputs   []string
	finished []int64
}

func (e *recordingEngine) StartFlow(ctx context.Context, flowID int64) error {
	e.mx.Lock()
	defer e.mx.Unlock()
	e.started = append(e.started, flowID)
	return e.err
}

func (e *recordingEngine) SignalInput(ctx context.Context, _ int64, input string) error {
	e.mx.Lock()
	defer e.mx.Unlock()
	e.inputs = append(e.inputs, input)
	return e.err
}

func (e *recordingEngine) FinishFlow(ctx context.Context, flowID int64) error {
	e.mx.Lock()
	defer e.mx.Unlock()
	e.finished = append(e.finished, flowID)
	return e.err
}

type durableTask struct {
	TaskWorker

	id        int64
	completed bool
	waiting   bool
	runs      atomic.Int32
	fed       bool
}

func (t *durableTask) GetTaskID() int64                          { return t.id }
func (t *durableTask) GetTitle() string                          { return "scan the host" }
func (t *durableTask) IsCompleted() bool                         { return t.completed }
func (t *durableTask) IsWaiting() bool                           { return t.waiting }
func (t *durableTask) GetResult(context.Context) (string, error) { return "", nil }
func (t *durableTask) GetStatus(context.Context) (database.TaskStatus, error) {
	return database.TaskStatusFinished, nil
}

func (t *durableTask) PutInput(context.Context, string) error {
	t.fed = true
	t.waiting = false
	return nil
}

func (t *durableTask) Run(ctx context.Context) error {
	t.runs.Add(1)
	return ctx.Err()
}

type durableTasks struct {
	TaskController

	mx      sync.Mutex
	tasks   []TaskWorker
	created []string
}

func (c *durableTasks) ListTasks(context.Context) []TaskWorker {
	c.mx.Lock()
	defer c.mx.Unlock()
	return slices.Clone(c.tasks)
}

func (c *durableTasks) GetTask(_ context.Context, taskID int64) (TaskWorker, error) {
	for _, task := range c.ListTasks(context.Background()) {
		if task.GetTaskID() == taskID {
			return task, nil
		}
	}
	return nil, errors.New("task not found")
}

func (c *durableTasks) CreateTask(ctx context.Context, input string, _ FlowUpdater) (TaskWorker, error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.created = append(c.created, input)
	task := &durableTask{id: 100, completed: true}
	c.tasks = append(c.tasks, task)
	return task, nil
}

type durableQuerier struct {
	createFakeQuerier

	taskIDs []int64
}

func (q *durableQuerier) GetFlowTasks(ctx context.Context, _ int64) ([]database.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tasks := make([]database.Task, 0, len(q.taskIDs))
	for _, id := range q.taskIDs {
		tasks = append(tasks, database.Task{ID: id})
	}
	return tasks, nil
}

func newDurableWorker(t *testing.T, tc TaskController, engine FlowEngine, taskIDs ...int64) (*flowWorker, temporal.FlowRunner) {
	t.Helper()

	q := &durableQuerier{taskIDs: taskIDs}
	q.flow = database.Flow{ID: reservedFlowID, Status: database.FlowStatusWaiting}
	pub := &cascadeFakePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	fw := &flowWorker{
		tc:      tc,
		wg:      &sync.WaitGroup{},
		ctx:     ctx,
		cancel:  cancel,
		taskMX:  &sync.Mutex{},
		taskST:  func() {},
		taskWG:  &sync.WaitGroup{},
		taskCCH: make(chan struct{}),
		input:   make(chan flowInput),
		engine:  engine,
		flowCtx: &FlowContext{
			DB:        q,
			FlowID:    reservedFlowID,
			Publisher: pub,
			MsgLog:    NewFlowMsgLogWorker(q, reservedFlowID, pub),
		},
		logger: logrus.WithField("flow_id", reservedFlowID),
	}

	fc, _, _ := newFinishController(q)
	fc.register(reservedFlowID, fw)
	runner, err := NewDurableRunner(fc)
	require.NoError(t, err)

	return fw, runner
}

func TestDurable_PutInput_QueuesInputInEngine(t *testing.T) {
	engine := &recordingEngine{}
	tc := &durableTasks{}
	fw, _ := newDurableWorker(t, tc, engine)

	require.NoError(t, fw.PutInput(context.Background(), "scan example.com", nil, nil))

	assert.Equal(t, []string{"scan example.com"}, engine.inputs)
	assert.Empty(t, tc.created)
}

func TestDurable_PutInput_EngineFailureIsNotAccepted(t *testing.T) {
	fw, _ := newDurableWorker(t, &durableTasks{}, &recordingEngine{err: errors.New("temporal unavailable")})

	err := fw.PutInput(context.Background(), "scan example.com", nil, nil)

	require.ErrorIs(t, err, ErrInputNotAccepted)
	assert.ErrorContains(t, err, "temporal unavailable")
}

func TestDurable_Start_UsesEngineInsteadOfWorkerGoroutine(t *testing.T) {
	engine := &recordingEngine{}
	fw, _ := newDurableWorker(t, &durableTasks{}, engine)

	require.NoError(t, fw.start(context.Background()))

	assert.Equal(t, []int64{reservedFlowID}, engine.started)
	select {
	case fw.input <- flowInput{input: "scan", reply: newInputReply()}:
		t.Fatal("an in-process worker is reading input beside the engine")
	default:
	}
}

func TestDurable_Finish_SignalsEngine(t *testing.T) {
	engine := &recordingEngine{}
	fw := newFinishableWorker(&finishFakeQuerier{}, &callerFakeExecutor{})
	fw.engine = engine

	require.NoError(t, fw.Finish(context.Background()))

	assert.Equal(t, []int64{42}, engine.finished)
}

func TestDurable_ProcessFlowInput_CreatesTask(t *testing.T) {
	tc := &durableTasks{}
	_, runner := newDurableWorker(t, tc, &recordingEngine{})

	err := runner.ProcessFlowInput(context.Background(), temporal.InputRequest{FlowID: reservedFlowID, Input: "scan"})

	require.NoError(t, err)
	assert.Equal(t, []string{"scan"}, tc.created)
}

func TestDurable_ProcessFlowInput_Recover(t *testing.T) {
	cases := []struct {
		name        string
		taskIDs     []int64
		waitingID   int64
		task        *durableTask
		wantRuns    int32
		wantCreated []string
		wantFed     bool
	}{
		{"resumes the task the interrupted attempt created", []int64{5, 9}, 0, &durableTask{id: 9}, 1, nil, false},
		{"leaves a created task that already finished", []int64{5, 9}, 0, &durableTask{id: 9, completed: true}, 0, nil, false},
		{"leaves a created task waiting for the user", []int64{5, 9}, 0, &durableTask{id: 9, waiting: true}, 0, nil, false},
		{"processes an input whose created task did not load", []int64{5, 9}, 0, &durableTask{id: 5, completed: true}, 0, []string{"scan"}, false},
		{"resumes an older task the attempt fed", []int64{5}, 0, &durableTask{id: 5}, 1, nil, false},
		{"feeds a waiting task the input it never got", []int64{5}, 5, &durableTask{id: 5, waiting: true}, 1, nil, true},
		{"resumes a fed waiting task left running", []int64{5}, 5, &durableTask{id: 5}, 1, nil, false},
		{"leaves a fed waiting task that finished", []int64{5}, 5, &durableTask{id: 5, completed: true}, 0, nil, false},
		{"processes an input that left no trace", []int64{5}, 0, &durableTask{id: 5, completed: true}, 0, []string{"scan"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &durableTasks{tasks: []TaskWorker{tc.task}}
			_, runner := newDurableWorker(t, tasks, &recordingEngine{}, tc.taskIDs...)

			err := runner.ProcessFlowInput(context.Background(), temporal.InputRequest{
				FlowID: reservedFlowID, Input: "scan", Recover: true, LastTaskID: 5, WaitingTaskID: tc.waitingID,
			})

			require.NoError(t, err)
			assert.Equal(t, tc.wantRuns, tc.task.runs.Load())
			assert.Equal(t, tc.wantCreated, tasks.created)
			assert.Equal(t, tc.wantFed, tc.task.fed)
		})
	}
}

func TestDurable_ProcessFlowInput_ClosedFlow(t *testing.T) {
	fw, runner := newDurableWorker(t, &durableTasks{}, &recordingEngine{})
	fw.cancel()

	err := runner.ProcessFlowInput(context.Background(), temporal.InputRequest{FlowID: reservedFlowID, Input: "scan"})

	require.ErrorIs(t, err, temporal.ErrFlowClosed)
}

func TestDurable_ProcessFlowInput_UnknownFlowIsNotLoaded(t *testing.T) {
	_, runner := newDurableWorker(t, &durableTasks{}, &recordingEngine{})

	err := runner.ProcessFlowInput(context.Background(), temporal.InputRequest{FlowID: 777, Input: "scan"})

	require.ErrorIs(t, err, temporal.ErrFlowNotLoaded)
}

func TestDurable_ResumeFlow_ContinuesRunningTasks(t *testing.T) {
	running := &durableTask{id: 5}
	waiting := &durableTask{id: 6, waiting: true}
	_, runner := newDurableWorker(t, &durableTasks{tasks: []TaskWorker{running, waiting}}, &recordingEngine{})

	require.NoError(t, runner.ResumeFlow(context.Background(), reservedFlowID))

	assert.Equal(t, int32(1), running.runs.Load())
	assert.Equal(t, int32(0), waiting.runs.Load())
}

func TestDurable_ProcessFlowInput_ActivityCancelLeavesTaskRunning(t *testing.T) {
	started := make(chan struct{})
	task := &blockingTask{durableTask: durableTask{id: 9}, started: started}
	fw, runner := newDurableWorker(t, &durableTasks{tasks: []TaskWorker{task}}, &recordingEngine{}, 5, 9)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.ProcessFlowInput(ctx, temporal.InputRequest{
			FlowID: reservedFlowID, Input: "scan", Recover: true, LastTaskID: 5,
		})
	}()

	controllerReceive(t, started, "the task to start")
	cancel()
	select {
	case err := <-done:
		t.Fatalf("run returned on activity cancel: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	fw.cancel()
	require.ErrorIs(t, controllerReceive(t, done, "the run to return once the flow closes"), temporal.ErrFlowClosed)
}

func TestDurable_Finish_WaitsForRunningActivity(t *testing.T) {
	started := make(chan struct{})
	task := &blockingTask{durableTask: durableTask{id: 9}, started: started, unwind: 50 * time.Millisecond}
	fw, runner := newDurableWorker(t, &durableTasks{tasks: []TaskWorker{task}}, &recordingEngine{}, 5, 9)

	go func() {
		_ = runner.ProcessFlowInput(context.Background(), temporal.InputRequest{
			FlowID: reservedFlowID, Input: "scan", Recover: true, LastTaskID: 5,
		})
	}()
	controllerReceive(t, started, "the task to start")

	require.NoError(t, fw.finish())
	assert.True(t, task.exited.Load())
}

type blockingTask struct {
	durableTask

	started chan struct{}
	unwind  time.Duration
	exited  atomic.Bool
}

func (t *blockingTask) Run(ctx context.Context) error {
	close(t.started)
	<-ctx.Done()
	time.Sleep(t.unwind)
	t.exited.Store(true)
	return ctx.Err()
}
