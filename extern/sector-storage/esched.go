package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"golang.org/x/xerrors"
	"reflect"
	"strings"
	"sync"
	"time"
)

type eResources struct {
	Memory           uint64
	CPUs             int
	GPUs             int
	DiskSpace        int64
	DisableSwap      bool
	InheritDiskSpace int64
}

const eKiB = 1024
const eMiB = 1024 * eKiB
const eGiB = 1024 * eMiB

var eResourceTable = map[sealtasks.TaskType]map[abi.RegisteredSealProof]*eResources{
	sealtasks.TTAddPiece: {
		/* Here we keep the same constraint as PC1 */
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 128 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: 64 * eGiB * 11 / 10, DisableSwap: true},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 64 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: 32 * eGiB * 11 / 10, DisableSwap: true},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: eGiB, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * eKiB, CPUs: 1, GPUs: 0, DiskSpace: 2 * eKiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 8 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 8 * eMiB * 11 / 10},
	},
	sealtasks.TTPreCommit1: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 128 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: (64*14 + 1) * eGiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 64 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: (32*14 + 1) * eGiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: eGiB, CPUs: 1, GPUs: 0, DiskSpace: (512*14 + 512) * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * eKiB, CPUs: 1, GPUs: 0, DiskSpace: (2*14 + 2) * eKiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 16 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: (8*14 + 8) * eMiB},
	},
	sealtasks.TTPreCommit2: {
		/* Specially, for P2 at the different worker as PC1, it should add disk space of PC1 */
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 32 * eGiB, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: (64*14 + 1) * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 16 * eGiB, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: (32*14 + 1) * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: eGiB, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: (512*14 + 1) * eMiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * eKiB, CPUs: 1, GPUs: 0, DiskSpace: 2 * eMiB, InheritDiskSpace: (2*14 + 1) * eKiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 16 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 16 * eMiB, InheritDiskSpace: (8*14 + 1) * eMiB * 11 / 10},
	},
	sealtasks.TTCommit1: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 4 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 2 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 512 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 2 * eKiB, CPUs: 1, GPUs: 0, DiskSpace: 2 * eMiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 8 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 16 * eMiB},
	},
	sealtasks.TTCommit2: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 580 * eGiB, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 290 * eGiB, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 128 * eGiB, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 32 * eKiB, CPUs: 1, GPUs: 0, DiskSpace: 2 * eMiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 32 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 16 * eMiB},
	},
}

type eWorkerRequest struct {
	id              uint64
	sector          abi.SectorID
	taskType        sealtasks.TaskType
	prepare         WorkerAction
	work            WorkerAction
	ret             chan workerResponse
	ctx             context.Context
	memUsed         uint64
	cpuUsed         int
	gpuUsed         int
	diskUsed        int64
	requestTime     int64
	requestTimeRaw  time.Time
	inqueueTime     int64
	inqueueTimeRaw  time.Time
	startTime       int64
	preparedTime    int64
	preparedTimeRaw time.Time
	endTime         int64
	sel             WorkerSelector
}

var eschedDefaultTaskPriority = 99

var eTaskPriority = map[sealtasks.TaskType]int{
	sealtasks.TTFinalize:   1,
	sealtasks.TTFetch:      2,
	sealtasks.TTCommit1:    3,
	sealtasks.TTCommit2:    4,
	sealtasks.TTPreCommit2: 5,
	sealtasks.TTPreCommit1: 6,
}

type eWorkerReqTypedList struct {
	taskType sealtasks.TaskType
	tasks    []*eWorkerRequest
}

type eWorkerReqPriorityList struct {
	priority        int
	typedTasksQueue map[sealtasks.TaskType]*eWorkerReqTypedList
}

type eWorkerTaskPrepared struct {
	wid WorkerID
	req *eWorkerRequest
}

type eWorkerSyncTaskList struct {
	mutex sync.Mutex
	queue []*eWorkerRequest
}

type eWorkerHandle struct {
	priv                  interface{}
	wid                   WorkerID
	w                     Worker
	wt                    *workTracker
	info                  storiface.WorkerInfo
	memUsed               uint64
	cpuUsed               int
	gpuUsed               int
	diskUsed              int64
	diskTotal             int64
	priorityTasksQueue    map[int]*eWorkerReqPriorityList
	preparedTasks         *eWorkerSyncTaskList
	preparingTasks        *eWorkerSyncTaskList
	runningTasks          []*eWorkerRequest
	maxConcurrent         map[sealtasks.TaskType]int
	memoryConcurrentLimit int
	diskConcurrentLimit   int
}

const eschedTag = "esched"

type eRequestFinisher struct {
	resp *workerResponse
	req  *eWorkerRequest
	wid  WorkerID
}

type eTaskWorkerBinder struct {
	mutex  sync.Mutex
	binder map[abi.SectorNumber]string
}

type eWorkerBucket struct {
	spt                abi.RegisteredSealProof
	id                 int
	newWorker          chan *eWorkerHandle
	workers            []*eWorkerHandle
	reqQueue           *eRequestQueue
	schedulerWaker     chan struct{}
	schedulerRunner    chan struct{}
	reqFinisher        chan *eRequestFinisher
	notifier           chan struct{}
	dropWorker         chan WorkerID
	retRequest         chan *eWorkerRequest
	storageNotifier    chan eStoreAction
	droppedWorker      chan string
	storeIDs           map[stores.ID]struct{}
	taskWorkerBinder   *eTaskWorkerBinder
	taskCleaner        chan eWorkerCleanerParam
	taskCleanerHandler chan eWorkerCleanerParam
	workerStatsQuery   chan *eWorkerStatsParam
	workerJobsQuery    chan *eWorkerJobsParam
	closing            chan struct{}
	ticker             *time.Ticker
}

type eRequestQueue struct {
	reqs  map[sealtasks.TaskType][]*eWorkerRequest
	mutex sync.Mutex
}

const eschedAdd = "add"
const eschedDrop = "drop"

const eschedResStagePrepare = "prepare"
const eschedResStageRuntime = "runtime"

type eStoreStat struct {
	space int64
	URLs  []string
	local bool
}

type eWorkerAction struct {
	address string
	act     string
}

type EStorage struct {
	ctx            context.Context
	index          stores.SectorIndex
	indexInstance  *stores.Index
	storeIDs       map[stores.ID]eStoreStat
	workerNotifier chan eWorkerAction
	ls             *stores.Local
}

type eStoreAction struct {
	id   stores.ID
	act  string
	stat eStoreStat
}

const eschedInvalidWorkerID = -1

var eschedTaskBindWorker = map[sealtasks.TaskType]sealtasks.TaskType{
	sealtasks.TTAddPiece:   sealtasks.TTPreCommit1,
	sealtasks.TTPreCommit1: sealtasks.TTPreCommit2,
	sealtasks.TTPreCommit2: sealtasks.TTCommit1,
}

const eschedWorkerCleanAtPrepare = "prepare"
const eschedWorkerCleanAtFinish = "finish"
const eschedWorkerLocalAddress = "localhost"

type eWorkerCleanerParam struct {
	stage        string
	sectorNumber abi.SectorNumber
	taskType     sealtasks.TaskType
}

var escheTaskCachable = []sealtasks.TaskType{
	sealtasks.TTPreCommit2,
}

type eWorkerRequestCleaner struct {
	req *eWorkerRequest
	wid WorkerID
}

type eWorkerRequestCleanerQueue struct {
	mutex sync.Mutex
	queue map[abi.SectorNumber]map[sealtasks.TaskType]*eWorkerRequestCleaner
}

type eWorkerBigCacheCleanerQueue struct {
	mutex sync.Mutex
	queue map[abi.SectorNumber]map[sealtasks.TaskType]string
}

const eschedWorkerJobs = "worker_jobs"
const eschedWorkerStats = "worker_stats"

type eWorkerStatsParam struct {
	command string
	resp    chan map[uint64]storiface.WorkerStats
}

type eWorkerJobsParam struct {
	command string
	resp    chan map[uint64][]storiface.WorkerJob
}

type edispatcher struct {
	spt              abi.RegisteredSealProof
	nextWorker       WorkerID
	nextRequest      uint64
	newWorker        chan *eWorkerHandle
	dropWorker       chan WorkerID
	newRequest       chan *eWorkerRequest
	buckets          []*eWorkerBucket
	reqQueue         *eRequestQueue
	storage          *EStorage
	storageNotifier  chan eStoreAction
	droppedWorker    chan string
	taskCleaner      chan eWorkerCleanerParam
	closing          chan struct{}
	ctx              context.Context
	taskWorkerBinder *eTaskWorkerBinder
	workerStatsQuery chan *eWorkerStatsParam
	workerJobsQuery  chan *eWorkerJobsParam
}

const eschedWorkerBuckets = 10
const eschedUnassignedWorker = 99999999

func (sh *edispatcher) dumpStorageInfo(store *stores.StorageEntry) {
	log.Infof("Storage %v", store.Info().ID)
	log.Infof("  Weight: ---------- %v", store.Info().Weight)
	log.Infof("  Can Seal: -------- %v", store.Info().CanSeal)
	log.Infof("  Can Store: ------- %v", store.Info().CanStore)
	log.Infof("  URLs: ------------")
	for _, url := range store.Info().URLs {
		log.Infof("    + %s", url)
	}
	log.Infof("  Fs Stat: ---------")
	log.Infof("    Capacity: ------ %v", store.FsStat().Capacity)
	log.Infof("    Available: ----- %v", store.FsStat().Available)
	log.Infof("    Reserved: ------ %v", store.FsStat().Reserved)
}

func (sh *edispatcher) storeNotify(id stores.ID, stat eStoreStat, act string) {
	sh.storageNotifier <- eStoreAction{
		id:   id,
		act:  act,
		stat: stat,
	}
}

func (sh *edispatcher) isLocalStorage(id stores.ID) bool {
	l, err := sh.storage.ls.Local(sh.ctx)
	if nil != err {
		log.Errorf("<%s> cannot get local storage", eschedTag)
		return false
	}
	for _, st := range l {
		if id == st.ID {
			return true
		}
	}
	return false
}

func (sh *edispatcher) checkStorageUpdate() {
	stors := sh.storage.indexInstance.Stores()
	for id, store := range stors {
		stor := (*stores.StorageEntry)(store)
		timeout := stor.LastHeartbeatTime().Add(10 * time.Minute)
		now := time.Now()
		if timeout.Before(now) {
			log.Errorf("<%s> delete storage %v to watcher [lost heartbeat]", eschedTag, id)
			sh.dumpStorageInfo(stor)
			sh.storeNotify(id, sh.storage.storeIDs[id], eschedDrop)
			delete(sh.storage.storeIDs, id)
			continue
		}
		if err := stor.HeartbeatError(); nil != err {
			log.Errorf("<%s> delete storage %v to watcher [%v | %v]", eschedTag, id, timeout, err)
			sh.dumpStorageInfo(stor)
			sh.storeNotify(id, sh.storage.storeIDs[id], eschedDrop)
			delete(sh.storage.storeIDs, id)
			continue
		}
		if _, ok := sh.storage.storeIDs[id]; ok {
			continue
		}
		sh.storage.storeIDs[id] = eStoreStat{
			space: stor.FsStat().Available,
			URLs:  stor.Info().URLs,
			local: sh.isLocalStorage(id),
		}
		log.Infof("<%s> add storage %v to watcher", eschedTag, id)
		sh.dumpStorageInfo(stor)
		sh.storeNotify(id, sh.storage.storeIDs[id], eschedAdd)
	}
}

func (sh *edispatcher) onNotifyStorageAddWorker(act eWorkerAction) {
	for id, stat := range sh.storage.storeIDs {
		sh.storeNotify(id, stat, eschedAdd)
	}
}

func (sh *edispatcher) onNotifyStorageDropWorker(act eWorkerAction) {
	for id, stat := range sh.storage.storeIDs {
		for _, url := range stat.URLs {
			if strings.Contains(url, act.address) {
				log.Infof("<%s> drop store %v by address %s", eschedTag, id, act.address)
				delete(sh.storage.storeIDs, id)
				break
			}
		}
	}
}

func (sh *edispatcher) onWorkerNotifyToStorage(act eWorkerAction) {
	switch act.act {
	case eschedAdd:
		sh.onNotifyStorageAddWorker(act)
	case eschedDrop:
		sh.onNotifyStorageDropWorker(act)
	}
}

func (sh *edispatcher) storageWatcher() {
	for {
		select {
		case <-sh.storage.indexInstance.StorageNotifier:
			sh.checkStorageUpdate()
		case act := <-sh.storage.workerNotifier:
			sh.onWorkerNotifyToStorage(act)
		}
	}
}

func (sh *edispatcher) SetStorage(storage *EStorage) {
	sh.storage = storage
	sh.storage.storeIDs = make(map[stores.ID]eStoreStat)
	sh.storage.workerNotifier = make(chan eWorkerAction)

	value := reflect.ValueOf(sh.storage.index)
	sh.storage.indexInstance = value.Interface().(*stores.Index)

	sh.checkStorageUpdate()
	go sh.storageWatcher()
}

func findTaskResource(spt abi.RegisteredSealProof, taskType sealtasks.TaskType) *eResources {
	return eResourceTable[taskType][spt]
}

func (res *eResources) dumpResources() {
	log.Debugf("Resource Information -------")
	log.Debugf("  CPUs: ---------------------- %v", res.CPUs)
	log.Debugf("  GPUs: ---------------------- %v", res.GPUs)
	log.Debugf("  Memory: -------------------- %v", res.Memory)
	log.Debugf("  Disk Space: ---------------- %v", res.DiskSpace)
}

func (w *eWorkerHandle) acquireRequestResource(req *eWorkerRequest, resType string) {
	switch resType {
	case eschedResStagePrepare:
		w.diskUsed += req.diskUsed
	case eschedResStageRuntime:
		w.cpuUsed += req.cpuUsed
		w.gpuUsed += req.gpuUsed
		w.memUsed += req.memUsed
	}
}

func (w *eWorkerHandle) releaseRequestResource(req *eWorkerRequest, resType string) {
	switch resType {
	case eschedResStagePrepare:
		w.diskUsed -= req.diskUsed
	case eschedResStageRuntime:
		w.cpuUsed -= req.cpuUsed
		w.gpuUsed -= req.gpuUsed
		w.memUsed -= req.memUsed
	}
}

func (bucket *eWorkerBucket) tryPeekAsManyRequests(worker *eWorkerHandle, taskType sealtasks.TaskType, tasksQueue *eWorkerReqTypedList, reqQueue *eRequestQueue) int {
	reqs := reqQueue.reqs[taskType]
	log.Debugf("<%s> %d %v tasks waiting for run [%s]", eschedTag, len(reqs), taskType, worker.info.Address)

	peekReqs := 0
	remainReqs := make([]*eWorkerRequest, 0)

	taskCount := 0
	worker.preparingTasks.mutex.Lock()
	taskCount += len(worker.preparingTasks.queue)
	worker.preparingTasks.mutex.Unlock()

	worker.preparedTasks.mutex.Lock()
	taskCount += len(worker.preparedTasks.queue)
	worker.preparedTasks.mutex.Unlock()

	taskCount += len(worker.runningTasks)
	for _, pq := range worker.priorityTasksQueue {
		for _, tq := range pq.typedTasksQueue {
			taskCount += len(tq.tasks)
		}
	}

	for {
		if 0 == len(reqs) || 0 == worker.diskConcurrentLimit {
			break
		}

		req := reqs[0]
		if worker.maxConcurrent[req.taskType] <= taskCount {
			log.Debugf("<%s> worker %s's %v tasks queue is full %d / %d",
				eschedTag, worker.info.Address, req.taskType,
				taskCount, worker.maxConcurrent[req.taskType])
			remainReqs = append(remainReqs, req)
			reqs = reqs[1:]
			continue
		}

		bucket.taskWorkerBinder.mutex.Lock()
		address, ok := bucket.taskWorkerBinder.binder[req.sector.Number]
		if ok {
			if address != worker.info.Address {
				bucket.taskWorkerBinder.mutex.Unlock()
				remainReqs = append(remainReqs, req)
				reqs = reqs[1:]
				continue
			}
		}
		bucket.taskWorkerBinder.mutex.Unlock()

		rpcCtx, cancel := context.WithTimeout(req.ctx, SelectorTimeout)
		ok, err := req.sel.Ok(rpcCtx, req.taskType, bucket.spt, worker)
		cancel()
		if err != nil {
			log.Debugf("<%s> cannot judge worker %s for task %v/%v status %v",
				eschedTag, worker.info.Address, req.sector, req.taskType, err)
			remainReqs = append(remainReqs, req)
			reqs = reqs[1:]
			continue
		}

		if !ok {
			log.Debugf("<%s> worker %s is not OK for task %v/%v",
				eschedTag, worker.info.Address, req.sector, req.taskType)
			remainReqs = append(remainReqs, req)
			reqs = reqs[1:]
			continue
		}

		req.inqueueTime = time.Now().UnixNano()
		req.inqueueTimeRaw = time.Now()
		worker.acquireRequestResource(req, eschedResStagePrepare)

		peekReqs += 1
		reqs = reqs[1:]
		tasksQueue.tasks = append(tasksQueue.tasks, req)
		taskCount += 1
	}

	reqQueue.reqs[taskType] = append(remainReqs, reqs[0:]...)

	return peekReqs
}

func (bucket *eWorkerBucket) tryPeekRequest() {
	log.Debugf("<%s> try peek schedule request from %d workers of bucket [%d]", eschedTag, len(bucket.workers), bucket.id)

	peekReqs := 0

	for _, worker := range bucket.workers {
		for _, pq := range worker.priorityTasksQueue {
			for taskType, tq := range pq.typedTasksQueue {
				bucket.reqQueue.mutex.Lock()
				if _, ok := bucket.reqQueue.reqs[taskType]; ok {
					if 0 < len(bucket.reqQueue.reqs[taskType]) {
						peekReqs += bucket.tryPeekAsManyRequests(worker, taskType, tq, bucket.reqQueue)
					}
				}
				bucket.reqQueue.mutex.Unlock()
			}
		}
	}

	if 0 < peekReqs {
		go func() { bucket.schedulerWaker <- struct{}{} }()
	}
}

func (bucket *eWorkerBucket) prepareTypedTask(worker *eWorkerHandle, task *eWorkerRequest) {
	log.Debugf("<%s> preparing typed task %v/%v to %s", eschedTag, task.sector, task.taskType, worker.info.Address)
	task.dumpWorkerRequest()

	task.startTime = time.Now().UnixNano()

	worker.preparingTasks.mutex.Lock()
	worker.preparingTasks.queue = append(worker.preparingTasks.queue, task)
	worker.preparingTasks.mutex.Unlock()

	err := task.prepare(task.ctx, worker.wt.worker(worker.w))

	worker.preparingTasks.mutex.Lock()
	for idx, req := range worker.preparingTasks.queue {
		if req.id == task.id {
			worker.preparingTasks.queue = append(worker.preparingTasks.queue[0:idx], worker.preparingTasks.queue[idx+1:]...)
			break
		}
	}
	worker.preparingTasks.mutex.Unlock()

	if nil != err {
		log.Errorf("<%s> cannot prepare typed task %v/%v/%s[%v]", eschedTag, task.sector, task.taskType, worker.info.Address, err)
		bucket.reqFinisher <- &eRequestFinisher{
			req:  task,
			resp: &workerResponse{err: err},
			wid:  worker.wid,
		}
		return
	}

	log.Debugf("<%s> prepared typed task %v/%v by %s", eschedTag, task.sector, task.taskType, worker.info.Address)
	task.preparedTime = time.Now().UnixNano()
	task.preparedTimeRaw = time.Now()

	worker.preparedTasks.mutex.Lock()
	worker.preparedTasks.queue = append(worker.preparedTasks.queue, task)
	worker.preparedTasks.mutex.Unlock()
	bucket.schedulerRunner <- struct{}{}

	canBindTaskType := false
	nextTaskType, ok := eschedTaskBindWorker[task.taskType]
	if ok {
		for _, taskType := range worker.info.SupportTasks {
			if taskType == nextTaskType {
				canBindTaskType = true
			}
		}
	}

	bucket.taskWorkerBinder.mutex.Lock()
	if canBindTaskType {
		bucket.taskWorkerBinder.binder[task.sector.Number] = worker.info.Address
	} else {
		delete(bucket.taskWorkerBinder.binder, task.sector.Number)
	}
	bucket.taskWorkerBinder.mutex.Unlock()
}

func (bucket *eWorkerBucket) runTypedTask(worker *eWorkerHandle, task *eWorkerRequest) {
	log.Debugf("<%s> executing typed task %v/%v at %s", eschedTag, task.sector, task.taskType, worker.info.Address)
	err := task.work(task.ctx, worker.wt.worker(worker.w))
	bucket.reqFinisher <- &eRequestFinisher{
		req:  task,
		resp: &workerResponse{err: err},
		wid:  worker.wid,
	}
	task.endTime = time.Now().UnixNano()
	log.Infof("<%s> finished typed task %v/%v duration (%dms / %d ms / %d ms) by %s [%v]",
		eschedTag, task.sector, task.taskType,
		(task.inqueueTime-task.requestTime)/1000000.0,
		(task.preparedTime-task.inqueueTime)/1000000.0,
		(task.endTime-task.preparedTime)/1000000.0,
		worker.info.Address, err)
}

func (bucket *eWorkerBucket) scheduleTypedTasks(worker *eWorkerHandle) {
	scheduled := false
	for priority := 0; priority <= eschedDefaultTaskPriority; priority++ {
		pq, ok := worker.priorityTasksQueue[priority]
		if !ok {
			continue
		}
		for _, typedTasks := range pq.typedTasksQueue {
			tasks := typedTasks.tasks
			for {
				if 0 == len(tasks) {
					break
				}
				task := tasks[0]
				go bucket.prepareTypedTask(worker, task)
				tasks = tasks[1:]
				scheduled = true
			}
			typedTasks.tasks = tasks
		}
		if scheduled {
			// Always run only one priority for each time
			return
		}
	}
}

func (bucket *eWorkerBucket) schedulePreparedTasks(worker *eWorkerHandle) {
	idleCpus := int(worker.info.Resources.CPUs * 4 / 100)
	if 0 == idleCpus {
		idleCpus = 1
	}

	for {
		worker.preparedTasks.mutex.Lock()
		if 0 == len(worker.preparedTasks.queue) {
			worker.preparedTasks.mutex.Unlock()
			break
		}
		task := worker.preparedTasks.queue[0]
		worker.preparedTasks.mutex.Unlock()

		taskType := task.taskType
		res := findTaskResource(bucket.spt, taskType)

		needCPUs := res.CPUs
		if 0 < res.GPUs {
			if 0 < len(worker.info.Resources.GPUs) {
				if len(worker.info.Resources.GPUs) < res.GPUs+worker.gpuUsed {
					log.Debugf("<%s> need %d = %d + %d GPUs but only %d available [%v]",
						eschedTag, res.GPUs+worker.gpuUsed, res.GPUs, worker.gpuUsed,
						len(worker.info.Resources.GPUs)-worker.gpuUsed, taskType)
					break
				}
			} else {
				needCPUs = int(worker.info.Resources.CPUs) - idleCpus
			}
		}
		if int(worker.info.Resources.CPUs)-idleCpus < needCPUs+worker.cpuUsed {
			log.Debugf("<%s> need %d = %d + %d CPUs but only %d available [%v]",
				eschedTag, needCPUs+worker.cpuUsed, needCPUs, worker.cpuUsed,
				int(worker.info.Resources.CPUs)-idleCpus, taskType)
			break
		}
		var extraMem uint64 = 0
		if !res.DisableSwap {
			extraMem = worker.info.Resources.MemSwap
		}
		if worker.info.Resources.MemPhysical+extraMem < res.Memory+worker.memUsed {
			log.Debugf("<%s> need %d = %d + %d memory but only %d available [%v]",
				eschedTag, res.Memory+worker.memUsed, res.Memory, worker.memUsed,
				worker.info.Resources.MemPhysical+extraMem, taskType)
			break
		}

		task.cpuUsed = needCPUs
		if 0 < len(worker.info.Resources.GPUs) {
			task.gpuUsed = res.GPUs
		}
		task.memUsed = res.Memory
		worker.acquireRequestResource(task, eschedResStageRuntime)

		worker.preparedTasks.mutex.Lock()
		worker.preparedTasks.queue = worker.preparedTasks.queue[1:]
		worker.preparedTasks.mutex.Unlock()

		worker.runningTasks = append(worker.runningTasks, task)
		go bucket.runTypedTask(worker, task)
	}
}

func (bucket *eWorkerBucket) scheduleBucketTask() {
	log.Debugf("<%s> try schedule bucket task for bucket [%d]", eschedTag, bucket.id)
	for _, worker := range bucket.workers {
		bucket.scheduleTypedTasks(worker)
	}
}

func (bucket *eWorkerBucket) schedulePreparedTask() {
	log.Debugf("<%s> try schedule prepared task for bucket [%d]", eschedTag, bucket.id)
	for _, worker := range bucket.workers {
		bucket.schedulePreparedTasks(worker)
	}
}

func (bucket *eWorkerBucket) findBucketWorkerByAddress(addr string) *eWorkerHandle {
	for _, worker := range bucket.workers {
		if addr == worker.info.Address {
			return worker
		}
	}
	return nil
}

func (bucket *eWorkerBucket) findBucketWorkerByID(wid WorkerID) *eWorkerHandle {
	for _, worker := range bucket.workers {
		if wid == worker.wid {
			return worker
		}
	}
	return nil
}

func (bucket *eWorkerBucket) findBucketWorkerIndexByID(wid WorkerID) int {
	for index, worker := range bucket.workers {
		if wid == worker.wid {
			return index
		}
	}
	return -1
}

func (bucket *eWorkerBucket) taskFinished(finisher *eRequestFinisher) {
	w := bucket.findBucketWorkerByID(finisher.wid)
	if nil != w {
		w.releaseRequestResource(finisher.req, eschedResStageRuntime)

		idx := -1
		for id, task := range w.runningTasks {
			if task.id == finisher.req.id {
				idx = id
				break
			}
		}

		if 0 <= idx {
			w.runningTasks = append(w.runningTasks[:idx], w.runningTasks[idx+1:]...)
		}
	}

	go func() { finisher.req.ret <- *finisher.resp }()
	go func() { bucket.notifier <- struct{}{} }()
	go func() { bucket.schedulerWaker <- struct{}{} }()
	go func() { bucket.schedulerRunner <- struct{}{} }()
}

func (bucket *eWorkerBucket) removeWorkerFromBucket(wid WorkerID) {
	log.Infof("<%s> try to drop worker %v from bucket %d", eschedTag, wid, bucket.id)
	worker := bucket.findBucketWorkerByID(wid)
	if nil != worker {
		for _, pq := range worker.priorityTasksQueue {
			for _, tq := range pq.typedTasksQueue {
				for {
					if 0 == len(tq.tasks) {
						break
					}
					task := tq.tasks[0]
					log.Infof("<%s> return typed task %v/%v to reqQueue", task.sector, task.taskType)
					tq.tasks = tq.tasks[1:]
					go func() { bucket.retRequest <- task }()
				}
			}
		}
		for {
			worker.preparedTasks.mutex.Lock()
			if 0 == len(worker.preparedTasks.queue) {
				worker.preparedTasks.mutex.Unlock()
				break
			}
			task := worker.preparedTasks.queue[0]
			log.Infof("<%s> finish task %v/%v", eschedTag, task.sector, task.taskType)
			worker.preparedTasks.queue = worker.preparedTasks.queue[1:]
			worker.preparedTasks.mutex.Unlock()

			bucket.reqFinisher <- &eRequestFinisher{
				req:  task,
				resp: &workerResponse{err: xerrors.Errorf("worker dropped unexpected")},
				wid:  worker.wid,
			}
		}
	}

	index := bucket.findBucketWorkerIndexByID(wid)
	if 0 <= index {
		bucket.workers = append(bucket.workers[:index], bucket.workers[index+1:]...)
		bucket.droppedWorker <- worker.info.Address
		log.Infof("<%s> drop worker %v from bucket %d", eschedTag, worker.info.Address, bucket.id)
	}
}

func (bucket *eWorkerBucket) appendNewWorker(w *eWorkerHandle) {
	bucket.workers = append(bucket.workers, w)
	log.Infof("<%s> new worker [%v] %s is added to bucket %d [%d]",
		eschedTag, w.wid, w.info.Address, bucket.id, len(bucket.workers))
	go func() { bucket.notifier <- struct{}{} }()
}

func (bucket *eWorkerBucket) getLocalWorker() *eWorkerHandle {
	for _, worker := range bucket.workers {
		if worker.info.Address == eschedWorkerLocalAddress {
			return worker
		}
	}
	return nil
}

func (bucket *eWorkerBucket) findWorkerByStoreURL(urls []string) *eWorkerHandle {
	for _, worker := range bucket.workers {
		for _, url := range urls {
			if strings.Contains(url, worker.info.Address) {
				return worker
			}
		}
	}
	return nil
}

func (bucket *eWorkerBucket) onAddStore(w *eWorkerHandle, act eStoreAction) {
	if _, ok := bucket.storeIDs[act.id]; ok {
		log.Infof("<%s> store %v already added", eschedTag, act.id)
		return
	}
	bucket.storeIDs[act.id] = struct{}{}
	w.diskTotal += act.stat.space

	var limit int = int(act.stat.space / eResourceTable[sealtasks.TTPreCommit1][bucket.spt].DiskSpace)
	w.diskConcurrentLimit += limit

	if w.diskConcurrentLimit < w.memoryConcurrentLimit {
		w.maxConcurrent[sealtasks.TTPreCommit1] = w.diskConcurrentLimit
		w.maxConcurrent[sealtasks.TTAddPiece] = w.diskConcurrentLimit
		w.maxConcurrent[sealtasks.TTPreCommit2] = w.diskConcurrentLimit

		log.Infof("<%s> update max concurrent for %v = %v [%s]",
			eschedTag,
			sealtasks.TTPreCommit1,
			w.maxConcurrent[sealtasks.TTPreCommit1],
			w.info.Address)
	} else {
		w.maxConcurrent[sealtasks.TTPreCommit1] = w.memoryConcurrentLimit
		w.maxConcurrent[sealtasks.TTAddPiece] = w.memoryConcurrentLimit
		w.maxConcurrent[sealtasks.TTPreCommit2] = w.memoryConcurrentLimit
		log.Infof("<%s> update max concurrent for %v = %v [%s]",
			eschedTag,
			sealtasks.TTPreCommit1,
			w.maxConcurrent[sealtasks.TTPreCommit1],
			w.info.Address)
	}
}

func (bucket *eWorkerBucket) onDropStore(w *eWorkerHandle, act eStoreAction) {
	if _, ok := bucket.storeIDs[act.id]; !ok {
		log.Infof("<%s> store %v already dropped", eschedTag, act.id)
		return
	}
	w.diskTotal -= act.stat.space
	delete(bucket.storeIDs, act.id)
}

func (bucket *eWorkerBucket) onStorageNotify(act eStoreAction) {
	worker := bucket.findWorkerByStoreURL(act.stat.URLs)
	if nil == worker {
		if act.stat.local {
			worker = bucket.getLocalWorker()
		}
	}
	if nil == worker {
		return
	}
	log.Infof("<%s> %v store %v [bucket %d / worker %s]", eschedTag, act.act, act.id, bucket.id, worker.info.Address)
	switch act.act {
	case eschedAdd:
		bucket.onAddStore(worker, act)
	case eschedDrop:
		bucket.onDropStore(worker, act)
	}
	go func() { bucket.notifier <- struct{}{} }()
}

func (bucket *eWorkerBucket) onTaskClean(param eWorkerCleanerParam) {
	// TODO
}

func (bucket *eWorkerBucket) onWorkerStatsQuery(param *eWorkerStatsParam) {
	out := map[uint64]storiface.WorkerStats{}

	for _, worker := range bucket.workers {
		out[uint64(worker.wid)] = storiface.WorkerStats{
			Info:    worker.info,
			GpuUsed: 0 < worker.gpuUsed,
			CpuUse:  uint64(worker.cpuUsed),
			Tasks:   make(map[sealtasks.TaskType]storiface.TasksInfo),
		}
		for _, taskType := range worker.info.SupportTasks {
			out[uint64(worker.wid)].Tasks[taskType] = storiface.TasksInfo{
				Waiting:       0,
				Running:       0,
				Prepared:      0,
				MaxConcurrent: worker.maxConcurrent[taskType],
			}
		}
		for _, pq := range worker.priorityTasksQueue {
			for taskType, tq := range pq.typedTasksQueue {
				info := out[uint64(worker.wid)].Tasks[taskType]
				info.Waiting += len(tq.tasks)
				out[uint64(worker.wid)].Tasks[taskType] = info
			}
		}
		worker.preparingTasks.mutex.Lock()
		for _, task := range worker.preparingTasks.queue {
			info := out[uint64(worker.wid)].Tasks[task.taskType]
			info.Prepared += 1
			out[uint64(worker.wid)].Tasks[task.taskType] = info
		}
		worker.preparingTasks.mutex.Unlock()
		worker.preparedTasks.mutex.Lock()
		for _, task := range worker.preparedTasks.queue {
			info := out[uint64(worker.wid)].Tasks[task.taskType]
			info.Prepared += 1
			out[uint64(worker.wid)].Tasks[task.taskType] = info
		}
		worker.preparedTasks.mutex.Unlock()
		for _, task := range worker.runningTasks {
			info := out[uint64(worker.wid)].Tasks[task.taskType]
			info.Running += 1
			out[uint64(worker.wid)].Tasks[task.taskType] = info
		}
	}

	go func() { param.resp <- out }()
}

func (bucket *eWorkerBucket) onWorkerJobsQuery(param *eWorkerJobsParam) {
	out := map[uint64][]storiface.WorkerJob{}

	for _, worker := range bucket.workers {
		for _, task := range worker.runningTasks {
			out[uint64(worker.wid)] = append(out[uint64(worker.wid)], storiface.WorkerJob{
				ID:     task.id,
				Sector: task.sector,
				Task:   task.taskType,
				Start:  task.preparedTimeRaw,
			})
		}

		wi := 0
		worker.preparedTasks.mutex.Lock()
		for _, task := range worker.preparedTasks.queue {
			out[uint64(worker.wid)] = append(out[uint64(worker.wid)], storiface.WorkerJob{
				ID:      task.id,
				Sector:  task.sector,
				Task:    task.taskType,
				RunWait: wi + 1,
				Start:   task.inqueueTimeRaw,
			})
			wi += 1
		}
		worker.preparedTasks.mutex.Unlock()

		worker.preparingTasks.mutex.Lock()
		for _, task := range worker.preparingTasks.queue {
			out[uint64(worker.wid)] = append(out[uint64(worker.wid)], storiface.WorkerJob{
				ID:      task.id,
				Sector:  task.sector,
				Task:    task.taskType,
				RunWait: wi + 1,
				Start:   task.inqueueTimeRaw,
			})
			wi += 1
		}
		worker.preparingTasks.mutex.Unlock()

		wi += 100000
		for priority, pq := range worker.priorityTasksQueue {
			for _, tq := range pq.typedTasksQueue {
				for _, task := range tq.tasks {
					out[uint64(worker.wid)] = append(out[uint64(worker.wid)], storiface.WorkerJob{
						ID:      task.id,
						Sector:  task.sector,
						Task:    task.taskType,
						RunWait: wi*priority + 1,
						Start:   task.inqueueTimeRaw,
					})
					wi += 1
				}
			}
		}
	}

	go func() { param.resp <- out }()
}

func (bucket *eWorkerBucket) onScheduleTick() {
	// go func() { bucket.notifier <- struct{}{} }()
}

func (bucket *eWorkerBucket) scheduler() {
	log.Infof("<%s> run scheduler for bucket %d", eschedTag, bucket.id)

	for {
		select {
		case w := <-bucket.newWorker:
			bucket.appendNewWorker(w)
		case <-bucket.notifier:
			bucket.tryPeekRequest()
		case <-bucket.schedulerWaker:
			bucket.scheduleBucketTask()
		case <-bucket.schedulerRunner:
			bucket.schedulePreparedTask()
		case finisher := <-bucket.reqFinisher:
			bucket.taskFinished(finisher)
		case wid := <-bucket.dropWorker:
			bucket.removeWorkerFromBucket(wid)
		case act := <-bucket.storageNotifier:
			bucket.onStorageNotify(act)
		case param := <-bucket.taskCleanerHandler:
			bucket.onTaskClean(param)
		case param := <-bucket.workerStatsQuery:
			bucket.onWorkerStatsQuery(param)
		case param := <-bucket.workerJobsQuery:
			bucket.onWorkerJobsQuery(param)
		case <-bucket.ticker.C:
			bucket.onScheduleTick()
		case <-bucket.closing:
			log.Infof("<%s> finish scheduler for bucket %d", eschedTag, bucket.id)
			return
		}
	}
}

func (bucket *eWorkerBucket) addNewWorker(w *eWorkerHandle) {
	for _, worker := range bucket.workers {
		if w.info.Address == worker.info.Address {
			log.Warnf("<%s> worker %s already in bucket, should we remove it firstly?", eschedTag, worker.info.Address)
			return
		}
	}
	bucket.newWorker <- w
}

func newExtScheduler(spt abi.RegisteredSealProof) *edispatcher {
	dispatcher := &edispatcher{
		spt:        spt,
		nextWorker: 0,
		newWorker:  make(chan *eWorkerHandle, 40),
		dropWorker: make(chan WorkerID, 10),
		newRequest: make(chan *eWorkerRequest, 1000),
		buckets:    make([]*eWorkerBucket, eschedWorkerBuckets),
		reqQueue: &eRequestQueue{
			reqs: make(map[sealtasks.TaskType][]*eWorkerRequest),
		},
		storageNotifier: make(chan eStoreAction, 10),
		droppedWorker:   make(chan string, 10),
		taskCleaner:     make(chan eWorkerCleanerParam, 10),
		closing:         make(chan struct{}, 2),
		taskWorkerBinder: &eTaskWorkerBinder{
			binder: make(map[abi.SectorNumber]string),
		},
		workerStatsQuery: make(chan *eWorkerStatsParam, 10),
		workerJobsQuery:  make(chan *eWorkerJobsParam, 10),
	}

	for i := range dispatcher.buckets {
		dispatcher.buckets[i] = &eWorkerBucket{
			spt:                spt,
			id:                 i,
			newWorker:          make(chan *eWorkerHandle),
			workers:            make([]*eWorkerHandle, 0),
			reqQueue:           dispatcher.reqQueue,
			schedulerWaker:     make(chan struct{}, 20),
			schedulerRunner:    make(chan struct{}, 20000),
			reqFinisher:        make(chan *eRequestFinisher),
			notifier:           make(chan struct{}),
			dropWorker:         make(chan WorkerID, 10),
			retRequest:         dispatcher.newRequest,
			storageNotifier:    make(chan eStoreAction, 10),
			droppedWorker:      dispatcher.droppedWorker,
			storeIDs:           make(map[stores.ID]struct{}),
			taskWorkerBinder:   dispatcher.taskWorkerBinder,
			taskCleaner:        dispatcher.taskCleaner,
			taskCleanerHandler: make(chan eWorkerCleanerParam, 10),
			workerStatsQuery:   make(chan *eWorkerStatsParam, 10),
			workerJobsQuery:    make(chan *eWorkerJobsParam, 10),
			closing:            make(chan struct{}, 2),
			ticker:             time.NewTicker(3 * 60 * time.Second),
		}
		go dispatcher.buckets[i].scheduler()
	}

	eResourceTable[sealtasks.TTUnseal] = eResourceTable[sealtasks.TTPreCommit1]
	eResourceTable[sealtasks.TTFinalize] = eResourceTable[sealtasks.TTCommit1]
	eResourceTable[sealtasks.TTFetch] = eResourceTable[sealtasks.TTCommit1]
	eResourceTable[sealtasks.TTReadUnsealed] = eResourceTable[sealtasks.TTCommit1]

	return dispatcher
}

func (sh *edispatcher) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.newRequest <- &eWorkerRequest{
		sector:   sector,
		taskType: taskType,
		prepare:  prepare,
		work:     work,
		ret:      ret,
		ctx:      ctx,
		sel:      sel,
	}:
	}

	select {
	case resp := <-ret:
		return resp.err
	}

	return nil
}

func (w *eWorkerHandle) dumpWorkerInfo() {
	log.Infof("Worker information -----------")
	log.Infof("  Address: ------------------- %v", w.info.Address)
	log.Infof("  Group: --------------------- %v", w.info.GroupName)
	log.Infof("  Support Tasks: -------------")
	for _, taskType := range w.info.SupportTasks {
		log.Infof("    + %v", taskType)
	}
	log.Infof("  Hostname: ------------------ %v", w.info.Hostname)
	log.Infof("  Mem Physical: -------------- %v", w.info.Resources.MemPhysical)
	log.Infof("  Mem Used: ------------------ %v", w.memUsed)
	log.Infof("  Mem Swap: ------------------ %v", w.info.Resources.MemSwap)
	log.Infof("  Mem Reserved: -------------- %v", w.info.Resources.MemReserved)
	log.Infof("  CPUs: ---------------------- %v", w.info.Resources.CPUs)
	log.Infof("  CPU Used: ------------------ %v", w.cpuUsed)
	log.Infof("  GPUs------------------------")
	for _, gpu := range w.info.Resources.GPUs {
		log.Infof("    + %s", gpu)
	}
	log.Infof("  GPU Used: ------------------ %v", w.gpuUsed)
	log.Infof("  Disk Used: ----------------- %v", w.diskUsed)
}

func (w *eWorkerHandle) patchLocalhost() {
	if w.info.Address == "" {
		w.info.Address = eschedWorkerLocalAddress
		w.info.Hostname = eschedWorkerLocalAddress
		w.info.GroupName = eschedWorkerLocalAddress
	}
}

func getTaskPriority(taskType sealtasks.TaskType) int {
	priority, ok := eTaskPriority[taskType]
	if !ok {
		priority = eschedDefaultTaskPriority
	}
	return priority
}

func (sh *edispatcher) addNewWorkerToBucket(w *eWorkerHandle) {
	w.patchLocalhost()

	log.Infof("<%s> add new worker %s to bucket", eschedTag, w.info.Address)
	w.dumpWorkerInfo()

	w.priorityTasksQueue = make(map[int]*eWorkerReqPriorityList)
	for _, taskType := range w.info.SupportTasks {
		priority := getTaskPriority(taskType)
		if _, ok := w.priorityTasksQueue[priority]; !ok {
			w.priorityTasksQueue[priority] = &eWorkerReqPriorityList{
				typedTasksQueue: make(map[sealtasks.TaskType]*eWorkerReqTypedList),
				priority:        priority,
			}
		}
		w.priorityTasksQueue[priority].typedTasksQueue[taskType] = &eWorkerReqTypedList{
			taskType: taskType,
			tasks:    make([]*eWorkerRequest, 0),
		}
	}

	w.runningTasks = make([]*eWorkerRequest, 0)
	w.preparedTasks = &eWorkerSyncTaskList{
		queue: make([]*eWorkerRequest, 0),
	}
	w.preparingTasks = &eWorkerSyncTaskList{
		queue: make([]*eWorkerRequest, 0),
	}
	w.maxConcurrent = make(map[sealtasks.TaskType]int)

	var limit1 int = int(w.info.Resources.MemPhysical / eResourceTable[sealtasks.TTPreCommit1][sh.spt].Memory)
	var limit2 int = int(w.info.Resources.CPUs * 90 / 100)
	w.memoryConcurrentLimit = limit1
	w.maxConcurrent[sealtasks.TTPreCommit1] = limit1
	if limit2 < w.maxConcurrent[sealtasks.TTPreCommit1] {
		w.maxConcurrent[sealtasks.TTPreCommit1] = limit2
	}

	w.maxConcurrent[sealtasks.TTAddPiece] = w.maxConcurrent[sealtasks.TTPreCommit1]
	w.maxConcurrent[sealtasks.TTUnseal] = w.maxConcurrent[sealtasks.TTPreCommit1]

	log.Infof("<%s> max concurrent for %v = %v [%s]",
		eschedTag,
		sealtasks.TTPreCommit1,
		w.maxConcurrent[sealtasks.TTPreCommit1],
		w.info.Address)

	w.maxConcurrent[sealtasks.TTPreCommit2] = 4 * len(w.info.Resources.GPUs)
	if w.maxConcurrent[sealtasks.TTPreCommit2] < 8 {
		w.maxConcurrent[sealtasks.TTPreCommit2] = 8
	}
	log.Infof("<%s> max concurrent for %v = %v [%s]",
		eschedTag,
		sealtasks.TTPreCommit2,
		w.maxConcurrent[sealtasks.TTPreCommit2],
		w.info.Address)

	w.maxConcurrent[sealtasks.TTCommit1] = 1280
	w.maxConcurrent[sealtasks.TTFinalize] = 1280
	w.maxConcurrent[sealtasks.TTFetch] = 1280
	w.maxConcurrent[sealtasks.TTReadUnsealed] = 1280

	w.maxConcurrent[sealtasks.TTCommit2] = len(w.info.Resources.GPUs)
	var limit int = int((w.info.Resources.MemPhysical + w.info.Resources.MemSwap) / eResourceTable[sealtasks.TTCommit2][sh.spt].Memory)
	if limit < w.maxConcurrent[sealtasks.TTCommit2] {
		w.maxConcurrent[sealtasks.TTCommit2] = limit
	}
	log.Infof("<%s> max concurrent for %v = %v [%s]",
		eschedTag,
		sealtasks.TTCommit2,
		w.maxConcurrent[sealtasks.TTCommit2],
		w.info.Address)

	workerBucketIndex := w.wid % eschedWorkerBuckets
	bucket := sh.buckets[workerBucketIndex]

	bucket.addNewWorker(w)
	sh.storage.workerNotifier <- eWorkerAction{act: eschedAdd, address: w.info.Address}
}

func (req *eWorkerRequest) dumpWorkerRequest() {
	log.Debugf("Task Information -------")
	log.Debugf("  Sector ID: --------------- %v", req.sector)
	log.Debugf("  Task Type: --------------- %v", req.taskType)
}

func (sh *edispatcher) addNewWorkerRequestToBucketWorker(req *eWorkerRequest) {
	log.Infof("<%s> add new request %v/%v to request queue", eschedTag, req.sector, req.taskType)
	req.dumpWorkerRequest()

	req.id = sh.nextRequest
	sh.nextRequest += 1
	req.requestTime = time.Now().UnixNano()
	req.requestTimeRaw = time.Now()

	sh.reqQueue.mutex.Lock()
	if _, ok := sh.reqQueue.reqs[req.taskType]; !ok {
		sh.reqQueue.reqs[req.taskType] = make([]*eWorkerRequest, 0)
	}
	sh.reqQueue.reqs[req.taskType] = append(sh.reqQueue.reqs[req.taskType], req)
	sh.reqQueue.mutex.Unlock()

	go func() {
		for _, bucket := range sh.buckets {
			bucket.notifier <- struct{}{}
		}
	}()
}

func (sh *edispatcher) dropWorkerFromBucket(wid WorkerID) {
	log.Infof("<%s> drop worker %v from all bucket", eschedTag, wid)
	go func() {
		for _, bucket := range sh.buckets {
			bucket.dropWorker <- wid
		}
	}()
}

func (sh *edispatcher) onStorageNotify(act eStoreAction) {
	log.Infof("<%s> %v store %v", eschedTag, act.act, act.id)
	go func() {
		for _, bucket := range sh.buckets {
			bucket.storageNotifier <- act
		}
	}()
}

func (sh *edispatcher) onWorkerDropped(address string) {
	sh.taskWorkerBinder.mutex.Lock()
	for sector, binder := range sh.taskWorkerBinder.binder {
		if binder == address {
			delete(sh.taskWorkerBinder.binder, sector)
		}
	}
	sh.taskWorkerBinder.mutex.Unlock()
	sh.storage.workerNotifier <- eWorkerAction{act: eschedDrop, address: address}
}

func (sh *edispatcher) closeAllBuckets() {
	go func() {
		for _, bucket := range sh.buckets {
			bucket.closing <- struct{}{}
		}
	}()
}

func (sh *edispatcher) watchWorkerClosing(closing <-chan struct{}, wid WorkerID) {
	select {
	case <-closing:
		sh.dropWorker <- wid
	}
}

func (sh *edispatcher) addWorkerClosingWatcher(w *eWorkerHandle) {
	workerClosing, err := w.w.Closing(sh.ctx)
	if nil != err {
		log.Errorf("<%s> fail to watch worker closing for [%v] %s", eschedTag, w.wid, w.info.Address)
		return
	}
	// It's ugly but I think it's not a problem because most of the time the watcher is sleep
	go sh.watchWorkerClosing(workerClosing, w.wid)
}

func (sh *edispatcher) onTaskClean(param eWorkerCleanerParam) {
	go func() {
		for _, bucket := range sh.buckets {
			bucket.taskCleanerHandler <- param
		}
	}()
}

func (sh *edispatcher) onWorkerJobsQuery(param *eWorkerJobsParam) {
	go func(param *eWorkerJobsParam) {
		out := map[uint64][]storiface.WorkerJob{}
		for _, bucket := range sh.buckets {
			resp := make(chan map[uint64][]storiface.WorkerJob)
			queryParam := &eWorkerJobsParam{
				command: param.command,
				resp:    resp,
			}
			bucket.workerJobsQuery <- queryParam
			select {
			case r := <-resp:
				for k, v := range r {
					out[k] = v
				}
			}
		}
		sh.reqQueue.mutex.Lock()
		wi := 5000000
		for taskType, reqs := range sh.reqQueue.reqs {
			if _, ok := out[eschedUnassignedWorker]; !ok {
				out[uint64(eschedUnassignedWorker)] = make([]storiface.WorkerJob, 0)
			}
			for _, req := range reqs {
				out[uint64(eschedUnassignedWorker)] = append(out[uint64(eschedUnassignedWorker)], storiface.WorkerJob{
					ID:      req.id,
					Sector:  req.sector,
					Task:    taskType,
					RunWait: wi + 1,
					Start:   req.requestTimeRaw,
				})
				wi += 1
			}
		}
		sh.reqQueue.mutex.Unlock()
		param.resp <- out
	}(param)
}

func (sh *edispatcher) onWorkerStatsQuery(param *eWorkerStatsParam) {
	go func(param *eWorkerStatsParam) {
		out := map[uint64]storiface.WorkerStats{}
		for _, bucket := range sh.buckets {
			resp := make(chan map[uint64]storiface.WorkerStats)
			queryParam := &eWorkerStatsParam{
				command: param.command,
				resp:    resp,
			}
			bucket.workerStatsQuery <- queryParam
			select {
			case r := <-resp:
				for k, v := range r {
					out[k] = v
				}
			}
		}
		param.resp <- out
	}(param)
}

func (sh *edispatcher) runSched() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	sh.ctx = ctx

	for {
		select {
		case req := <-sh.newRequest:
			sh.addNewWorkerRequestToBucketWorker(req)
		case w := <-sh.newWorker:
			sh.addWorkerClosingWatcher(w)
			sh.addNewWorkerToBucket(w)
		case wid := <-sh.dropWorker:
			sh.dropWorkerFromBucket(wid)
		case act := <-sh.storageNotifier:
			sh.onStorageNotify(act)
		case address := <-sh.droppedWorker:
			sh.onWorkerDropped(address)
		case param := <-sh.taskCleaner:
			sh.onTaskClean(param)
		case param := <-sh.workerStatsQuery:
			sh.onWorkerStatsQuery(param)
		case param := <-sh.workerJobsQuery:
			sh.onWorkerJobsQuery(param)
		case <-sh.closing:
			sh.closeAllBuckets()
			return
		}
	}
}

func (sh *edispatcher) Info(ctx context.Context) (interface{}, error) {
	return nil, nil
}

func (sh *edispatcher) Close(ctx context.Context) error {
	sh.closing <- struct{}{}
	return nil
}

func (sh *edispatcher) NewWorker(w *eWorkerHandle) {
	w.wid = sh.nextWorker
	sh.nextWorker += 1
	sh.newWorker <- w
}

func (sh *edispatcher) WorkerStats() map[uint64]storiface.WorkerStats {
	resp := make(chan map[uint64]storiface.WorkerStats)
	param := &eWorkerStatsParam{
		command: eschedWorkerStats,
		resp:    resp,
	}
	sh.workerStatsQuery <- param
	select {
	case out := <-resp:
		return out
	}
}

func (sh *edispatcher) WorkerJobs() map[uint64][]storiface.WorkerJob {
	resp := make(chan map[uint64][]storiface.WorkerJob)
	param := &eWorkerJobsParam{
		command: eschedWorkerJobs,
		resp:    resp,
	}
	sh.workerJobsQuery <- param
	select {
	case out := <-resp:
		return out
	}
}

func (sh *edispatcher) MoveCacheDone(sector abi.SectorID) {
	// TODO: when cache moving done, add the space to the worker
}
