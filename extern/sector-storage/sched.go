package sectorstorage

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 4
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	spt abi.RegisteredSealProof

	workersLk  sync.RWMutex
	nextWorker WorkerID
	workers    map[WorkerID]*workerHandle

	newWorkers chan *workerHandle

	watchClosing  chan WorkerID
	workerClosing chan WorkerID

	schedule   chan *workerRequest
	reschedule chan *workerRequest

	windowRequests chan *schedWindowRequest

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing

	sectorWorkerGroup      map[abi.SectorNumber]string
	sectorWorkerGroupMutex sync.Mutex
}

type workerHandle struct {
	w Worker

	info      storiface.WorkerInfo
	taskRatio float32

	preparing *activeResources
	active    *activeResources

	prepareLk sync.Mutex
	lk        sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	// stats / tracking
	wt *workTracker

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64
	taskUsed   uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   abi.SectorID
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler(spt abi.RegisteredSealProof) *scheduler {
	return &scheduler{
		spt: spt,

		nextWorker: 0,
		workers:    map[WorkerID]*workerHandle{},

		newWorkers: make(chan *workerHandle),

		watchClosing:  make(chan WorkerID),
		workerClosing: make(chan WorkerID),

		schedule:   make(chan *workerRequest, 10),
		reschedule: make(chan *workerRequest, 1000),

		windowRequests: make(chan *schedWindowRequest, 20),

		schedQueue: &requestQueue{},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),

		sectorWorkerGroup: make(map[abi.SectorNumber]string),
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []WorkerID
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)

	go sh.runWorkerWatcher()

	iw := time.After(InitWait)
	var initialised bool

	intv := 6 * time.Minute
	timeout := time.NewTimer(intv)
	defer timeout.Stop()

	for {
		var doSched bool

		select {
		case w := <-sh.newWorkers:
			sh.newWorker(w)
			doSched = true

		case wid := <-sh.workerClosing:
			sh.dropWorker(wid)

		case req := <-sh.schedule:
			sh.schedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.openWindows = append(sh.openWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-timeout.C:
			doSched = true
			timeout.Reset(intv)
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case req := <-sh.schedule:
					sh.schedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.reschedule:
					sh.schedQueue.Push(req)
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			sh.trySched()
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, window.worker)
	}

	return out
}

func (sh *scheduler) trySched() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	windows := make([]schedWindow, len(sh.openWindows))
	acceptableWindows := make([][]int, sh.schedQueue.Len())

	log.Debugf("SCHED %d queued; %d open windows", sh.schedQueue.Len(), len(windows))

	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	if len(sh.openWindows) == 0 {
		// nothing to schedule on
		return
	}

	// Step 1
	concurrency := len(sh.openWindows)
	throttle := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	wg.Add(sh.schedQueue.Len())

	for i := 0; i < sh.schedQueue.Len(); i++ {
		throttle <- struct{}{}

		go func(sqi int) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]
			needRes := ResourceTable[task.taskType][sh.spt]

			task.indexHeap = sqi
			log.Debugf("find window for sector %v / %v", task.sector.Number, task.taskType)

			for wnd, windowRequest := range sh.openWindows {
				worker, ok := sh.workers[windowRequest.worker]
				if !ok {
					// log.Errorf("worker referenced by windowRequest not found (worker: %d)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info.Resources) {
					// log.Debugf("window %v worker %v cannot process sector %v / %v", wnd, worker.info.Address, task.sector.Number, task.taskType)
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, sh.spt, worker)
				cancel()
				if err != nil {
					// log.Debugf("window %v worker %v ERR for sector %v / %v [%v]", wnd, worker.info.Address, task.sector.Number, task.taskType, err)
					continue
				}

				if !ok {
					// log.Debugf("window %v worker %v NOK for sector %v / %v", wnd, worker.info.Address, task.sector.Number, task.taskType)
					continue
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
			}

			if len(acceptableWindows[sqi]) == 0 {
				log.Debugf("cannot find any window for sector %v / %v", task.sector.Number, task.taskType)
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good)
			rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
				acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
			})
			sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool {
				wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
				wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

				if wii == wji {
					// for the same worker prefer older windows
					return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
				}

				wi := sh.workers[wii]
				wj := sh.workers[wji]

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()

				r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj)
				if err != nil {
					log.Error("selecting best worker: %s", err)
				}
				return r
			})
			log.Debugf("found windows[%v] for sector %v / %v", len(acceptableWindows[sqi]), task.sector.Number, task.taskType)
		}(i)
	}

	wg.Wait()

	// Step 2
	scheduled := 0
	minWindowTodos := make([]int, len(windows))

	allPC2Task := 0

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]
		if sealtasks.TTPreCommit2 == task.taskType {
			allPC2Task += 1
		}
	}

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]
		needRes := ResourceTable[task.taskType][sh.spt]

		selectedWindow := -1

		if sealtasks.TTPreCommit2 == task.taskType {
			var havestWid WorkerID = math.MaxUint64
			var havestRatio float32 = 0.0

			for _, wnd := range acceptableWindows[task.indexHeap] {
				wid := sh.openWindows[wnd].worker
				wr := sh.workers[wid].info.Resources

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
					continue
				}

				if havestRatio < sh.workers[wid].taskRatio {
					havestWid = wid
					havestRatio = sh.workers[wid].taskRatio
				}
			}

			for _, wnd := range acceptableWindows[task.indexHeap] {
				wid := sh.openWindows[wnd].worker
				wr := sh.workers[wid].info.Resources

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
					continue
				}

				sh.sectorWorkerGroupMutex.Lock()
				if group, ok := sh.sectorWorkerGroup[task.sector.Number]; ok {
					if group == sh.workers[wid].info.GroupName {
						if float32(allPC2Task)*sh.workers[wid].taskRatio < float32(len(windows[wnd].todo)) && math.MaxUint64 != uint64(havestWid) {
							selectedWindow = int(havestWid)
						} else {
							selectedWindow = wnd
						}
						sh.sectorWorkerGroupMutex.Unlock()
						break
					}
				}
				sh.sectorWorkerGroupMutex.Unlock()
			}
		}

		if selectedWindow < 0 {
			for _, wnd := range acceptableWindows[task.indexHeap] {
				wid := sh.openWindows[wnd].worker
				wr := sh.workers[wid].info.Resources

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
					continue
				}

				if sealtasks.TTPreCommit2 == task.taskType {
					if (float32(allPC2Task) * sh.workers[wid].taskRatio) < float32(len(windows[wnd].todo)) {
						log.Debugf("PC2 tasks[%d] already reach up limit[%f] of worker [%v]", len(windows[wnd].todo), float32(allPC2Task)*sh.workers[wid].taskRatio, sh.workers[wid].info.Address)
						continue
					}
				}

				if len(windows[wnd].todo) < minWindowTodos[wnd] || 0 == minWindowTodos[wnd] {
					selectedWindow = wnd
					minWindowTodos[wnd] = len(windows[wnd].todo)
					if 0 == minWindowTodos[wnd] {
						break
					}
				}
			}
		}

		if selectedWindow < 0 {
			log.Debugf("cannot find suitable window for sector %v / %v [available: %d]", task.sector.Number, task.taskType, len(acceptableWindows[task.indexHeap]))
			// all windows full
			continue
		}

		worker := sh.workers[sh.openWindows[selectedWindow].worker]
		wr := worker.info.Resources
		log.Infof("SCHED ASSIGNED sqi:%d sector %d task %s to window %d [%v]", sqi, task.sector.Number, task.taskType, selectedWindow, worker.info.Address)
		windows[selectedWindow].allocated.add(wr, needRes)
		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)
		minWindowTodos[selectedWindow]++

		sh.schedQueue.Remove(sqi)
		sqi--
		scheduled++
	}

	// Step 3

	if scheduled == 0 {
		return
	}
	////////////////////////////////////////
	// Rearrange the todos list of worker
	n := len(windows)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if sh.openWindows[i].worker == sh.openWindows[j].worker && len(windows[j].todo) > 0 {
				windows[i].todo = append(windows[i].todo, windows[j].todo...)
				windows[j].todo = windows[j].todo[0:0]
			}
		}
	}
	////////////////////////////////////////

	scheduledWindows := map[int]struct{}{}
	keepAliveWindows := map[int]struct{}{}

	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		for _, todo := range window.todo {
			if todo.taskType == sealtasks.TTPreCommit1 {
				keepAliveWindows[wnd] = struct{}{}
				break
			}
		}

		window := window // copy
		go func(done chan *schedWindow, window schedWindow) {
			select {
			case done <- &window:
			default:
				for _, todo := range window.todo {
					go func(todo *workerRequest) { sh.reschedule <- todo }(todo)
				}
				log.Error("expected sh.openWindows[wnd].done to be buffered")
			}
		}(sh.openWindows[wnd].done, window)
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, len(sh.openWindows)-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			if _, keepAlive := keepAliveWindows[wnd]; !keepAlive {
				continue
			}
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

func (sh *scheduler) runWorker(wid WorkerID) {
	var ready sync.WaitGroup
	ready.Add(1)
	defer ready.Wait()

	go func() {
		sh.workersLk.Lock()
		worker, found := sh.workers[wid]
		sh.workersLk.Unlock()

		ready.Done()

		if !found {
			panic(fmt.Sprintf("worker %d not found", wid))
		}

		defer close(worker.closedMgr)

		scheduledWindows := make(chan *schedWindow, SchedWindows*10)
		taskDone := make(chan struct{}, 1)
		windowsRequested := 0

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		workerClosing, err := worker.w.Closing(ctx)
		if err != nil {
			return
		}

		defer func() {
			log.Warnw("Worker closing", "workerid", wid)

			// TODO: close / return all queued tasks
		}()

		for {
			// ask for more windows if we need them
			for ; windowsRequested < SchedWindows; windowsRequested++ {
				select {
				case sh.windowRequests <- &schedWindowRequest{
					worker: wid,
					done:   scheduledWindows,
				}:
				case <-sh.closing:
					return
				case <-workerClosing:
					return
				case <-worker.closingMgr:
					return
				}
			}

			select {
			case w := <-scheduledWindows:
				worker.wndLk.Lock()
				worker.activeWindows = append(worker.activeWindows, w)
				worker.wndLk.Unlock()
			case <-taskDone:
				log.Debugw("task done", "workerid", wid)
			case <-sh.closing:
				return
			case <-workerClosing:
				return
			case <-worker.closingMgr:
				return
			}

			sh.workersLk.Lock()
			worker.wndLk.Lock()

			windowsRequested -= sh.workerCompactWindows(worker, wid)

		assignLoop:
			// process windows in order
			for len(worker.activeWindows) > 0 {
				firstWindow := worker.activeWindows[0]

				worker.lk.Lock()
				todos := make([]*workerRequest, 0)
				for _, todo := range firstWindow.todo {
					if sealtasks.TTFinalize == todo.taskType {
						todos = append(todos, todo)
					}
				}
				for _, todo := range firstWindow.todo {
					if sealtasks.TTCommit1 == todo.taskType {
						todos = append(todos, todo)
					}
				}
				for _, todo := range firstWindow.todo {
					if sealtasks.TTPreCommit2 == todo.taskType {
						todos = append(todos, todo)
					}
				}
				for _, todo := range firstWindow.todo {
					if sealtasks.TTPreCommit2 != todo.taskType && sealtasks.TTCommit1 != todo.taskType && sealtasks.TTFinalize != todo.taskType {
						todos = append(todos, todo)
					}
				}

				firstWindow.todo = nil
				firstWindow.todo = todos
				worker.lk.Unlock()

				// process tasks within a window, preferring tasks at lower indexes
				for len(firstWindow.todo) > 0 {
					tidx := -1

					worker.lk.Lock()
					for t, todo := range firstWindow.todo {
						needRes := ResourceTable[todo.taskType][sh.spt]
						if worker.preparing.canHandleRequest(needRes, wid, "startPreparing", worker.info.Resources) ||
							sealtasks.TTPreCommit2 == todo.taskType || sealtasks.TTCommit1 == todo.taskType || sealtasks.TTFinalize == todo.taskType {
							tidx = t
							break
						}
					}
					worker.lk.Unlock()

					if tidx == -1 {
						worker.lk.Lock()
						todos := make([]*workerRequest, 0)
						for _, todo := range firstWindow.todo {
							needRes := ResourceTable[todo.taskType][sh.spt]
							if !worker.preparing.canHandleRequest(needRes, wid, "startPreparing", worker.info.Resources) &&
								sealtasks.TTCommit1 != todo.taskType && sealtasks.TTFinalize != todo.taskType && sealtasks.TTPreCommit2 != todo.taskType {
								log.Infof("sector %v cannot be processed [%v / %v], reschedule", todo.sector.Number, todo.taskType, worker.info.Address)
								go func(todo *workerRequest) { sh.reschedule <- todo }(todo)
							} else {
								todos = append(todos, todo)
							}
						}
						firstWindow.todo = nil
						firstWindow.todo = todos
						worker.lk.Unlock()
						break assignLoop
					}

					todo := firstWindow.todo[tidx]

					log.Infof("assign worker sector %d / %v -> %v", todo.sector.Number, todo.taskType, worker.info.Address)
					sh.sectorWorkerGroupMutex.Lock()
					if _, ok := sh.sectorWorkerGroup[todo.sector.Number]; !ok {
						if sealtasks.TTPreCommit1 == todo.taskType {
							sh.sectorWorkerGroup[todo.sector.Number] = worker.info.GroupName
						}
					}

					if _, ok := sh.sectorWorkerGroup[todo.sector.Number]; !ok {
						log.Infof("  sector %v group %s, worker group %s", todo.sector.Number, sh.sectorWorkerGroup[todo.sector.Number], worker.info.GroupName)
					} else {
						log.Warnf("  sector %v not assigned any group, check if it's already do PC1", todo.sector.Number)
					}
					sh.sectorWorkerGroupMutex.Unlock()

					err := sh.assignWorker(taskDone, wid, worker, todo)
					if err != nil {
						log.Error("assignWorker error: %+v", err)
						go todo.respond(xerrors.Errorf("assignWorker error: %w", err))
					}

					// Note: we're not freeing window.allocated resources here very much on purpose
					copy(firstWindow.todo[tidx:], firstWindow.todo[tidx+1:])
					firstWindow.todo[len(firstWindow.todo)-1] = nil
					firstWindow.todo = firstWindow.todo[:len(firstWindow.todo)-1]

				}

				copy(worker.activeWindows, worker.activeWindows[1:])
				worker.activeWindows[len(worker.activeWindows)-1] = nil
				worker.activeWindows = worker.activeWindows[:len(worker.activeWindows)-1]

				windowsRequested--
			}

			worker.wndLk.Unlock()
			sh.workersLk.Unlock()
		}
	}()
}

func (sh *scheduler) workerCompactWindows(worker *workerHandle, wid WorkerID) int {
	// move tasks from older windows to newer windows if older windows
	// still can fit them
	if len(worker.activeWindows) > 1 {
		for wi, window := range worker.activeWindows[1:] {
			lower := worker.activeWindows[wi]
			var moved []int

			for ti, todo := range window.todo {
				needRes := ResourceTable[todo.taskType][sh.spt]
				if !lower.allocated.canHandleRequest(needRes, wid, "compactWindows", worker.info.Resources) {
					continue
				}

				moved = append(moved, ti)
				lower.todo = append(lower.todo, todo)
				lower.allocated.add(worker.info.Resources, needRes)
				window.allocated.free(worker.info.Resources, needRes)
			}

			if len(moved) > 0 {
				newTodo := make([]*workerRequest, 0, len(window.todo)-len(moved))
				for i, t := range window.todo {
					if len(moved) > 0 && moved[0] == i {
						moved = moved[1:]
						continue
					}

					newTodo = append(newTodo, t)
				}
				window.todo = newTodo
			}
		}
	}

	var compacted int
	var newWindows []*schedWindow

	for _, window := range worker.activeWindows {
		if len(window.todo) == 0 {
			compacted++
			continue
		}

		newWindows = append(newWindows, window)
	}

	worker.activeWindows = newWindows

	return compacted
}

func (sh *scheduler) assignWorker(taskDone chan struct{}, wid WorkerID, w *workerHandle, req *workerRequest) error {
	needRes := ResourceTable[req.taskType][sh.spt]

	w.lk.Lock()
	w.preparing.add(w.info.Resources, needRes)
	w.lk.Unlock()

	go func() {
		w.prepareLk.Lock()
		log.Infof("preparing sector %v / %v -> %v", req.sector.Number, req.taskType, w.info.Address)
		err := req.prepare(req.ctx, w.wt.worker(w.w))
		log.Infof("prepared sector %v / %v -> %v [%v]", req.sector.Number, req.taskType, w.info.Address, err)
		w.prepareLk.Unlock()

		sh.workersLk.Lock()

		if err != nil {
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()

			select {
			case taskDone <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		err = w.active.withResources(wid, w.info.Resources, needRes, &sh.workersLk, func() error {
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()
			defer sh.workersLk.Lock() // we MUST return locked from this function

			select {
			case taskDone <- struct{}{}:
			case <-sh.closing:
			}

			log.Infof("executing sector %v / %v -> %v", req.sector.Number, req.taskType, w.info.Address)
			err = req.work(req.ctx, w.wt.worker(w.w))

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

func (sh *scheduler) newWorker(w *workerHandle) {
	w.closedMgr = make(chan struct{})
	w.closingMgr = make(chan struct{})

	sh.workersLk.Lock()
	id := sh.nextWorker
	sh.workers[id] = w
	sh.nextWorker++

	allGPUs := 0
	for wid, _ := range sh.workers {
		info := sh.workers[wid].info
		find := false
		for _, tpy := range info.SupportTasks {
			if tpy == sealtasks.TTPreCommit2 {
				find = true
				break
			}
		}
		if find {
			if len(info.Resources.GPUs) > 0 {
				allGPUs += len(info.Resources.GPUs)
			} else {
				allGPUs += 1
			}
		}
	}

	if allGPUs > 0 {
		for wid, _ := range sh.workers {
			info := sh.workers[wid].info
			for _, tpy := range info.SupportTasks {
				if tpy == sealtasks.TTPreCommit2 {
					gpus := len(sh.workers[id].info.Resources.GPUs)
					if gpus <= 0 {
						gpus = 1
					}
					sh.workers[id].lk.Lock()
					sh.workers[id].taskRatio = float32(gpus) / float32(allGPUs)
					sh.workers[id].lk.Unlock()
					log.Warnf("set worker id %d task ratio is %f(%d/%d).", id, sh.workers[id].taskRatio, gpus, allGPUs)
				}
			}
		}
	}
	sh.workersLk.Unlock()

	sh.runWorker(id)

	select {
	case sh.watchClosing <- id:
	case <-sh.closing:
		return
	}
}

func (sh *scheduler) dropWorker(wid WorkerID) {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	w := sh.workers[wid]

	sh.workerCleanup(wid, w)

	delete(sh.workers, wid)
}

func (sh *scheduler) workerCleanup(wid WorkerID, w *workerHandle) {
	select {
	case <-w.closingMgr:
	default:
		close(w.closingMgr)
	}

	sh.workersLk.Unlock()
	select {
	case <-w.closedMgr:
	case <-time.After(time.Second):
		log.Errorf("timeout closing worker manager goroutine %d", wid)
	}
	sh.workersLk.Lock()

	if !w.cleanupStarted {
		w.cleanupStarted = true

		newWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
		for _, window := range sh.openWindows {
			if window.worker != wid {
				newWindows = append(newWindows, window)
			}
		}
		sh.openWindows = newWindows

		for _, window := range w.activeWindows {
			for _, todo := range window.todo {
				go func(todo *workerRequest) { sh.reschedule <- todo }(todo)
			}
		}

		log.Infof("dropWorker %d", wid)

		go func() {
			if err := w.w.Close(); err != nil {
				log.Warnf("closing worker %d: %+v", err)
			}
		}()
	}
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
