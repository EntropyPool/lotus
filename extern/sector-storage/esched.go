package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
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
	InheritDiskSpace int64
}

const eKiB = 1024
const eMiB = 1024 * eKiB
const eGiB = 1024 * eMiB

var eResourceTable = map[sealtasks.TaskType]map[abi.RegisteredSealProof]*eResources{
	sealtasks.TTAddPiece: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: eGiB, CPUs: 1, GPUs: 0, DiskSpace: 64 * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: eGiB, CPUs: 1, GPUs: 0, DiskSpace: 32 * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 64 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 2 * eKiB, CPUs: 1, GPUs: 0, DiskSpace: 2 * eKiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 8 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 8 * eMiB * 11 / 10},
	},
	sealtasks.TTPreCommit1: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 128 * 1024 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: (64*14 + 1) * eGiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 64 * 1024 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: (32*14 + 1) * eGiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 1024 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: (512*14 + 512) * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * 1024, CPUs: 1, GPUs: 0, DiskSpace: (2*14 + 2) * eKiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 16 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: (8*14 + 8) * eMiB},
	},
	sealtasks.TTPreCommit2: {
		/* Specially, for P2 at the different worker as P1, it should add disk space of P1 */
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 32 * 1024 * 1024 * 1024, CPUs: 2, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: 64 * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 16 * 1024 * 1024 * 1024, CPUs: 2, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: 32 * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 1024 * 1024 * 1024, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: 512 * eMiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 2 * eMiB, InheritDiskSpace: 2 * eKiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 16 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 16 * eMiB, InheritDiskSpace: 8 * eMiB * 11 / 10},
	},
	sealtasks.TTCommit1: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 4 * 1024 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 2 * 1024 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 512 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 2 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 2 * eMiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 8 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 16 * eMiB},
	},
	sealtasks.TTCommit2: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 580 * 1024 * 1024 * 1024, CPUs: 2, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 290 * 1024 * 1024 * 1024, CPUs: 2, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 8 * 1024 * 1024 * 1024, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 32 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 2 * eMiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 32 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 16 * eMiB},
	},
}

type eWorkerRequest struct {
	id           uint64
	sector       abi.SectorID
	taskType     sealtasks.TaskType
	prepare      WorkerAction
	work         WorkerAction
	ret          chan workerResponse
	ctx          context.Context
	memUsed      uint64
	cpuUsed      int
	gpuUsed      int
	diskUsed     int64
	inqueueTime  int64
	startTime    int64
	preparedTime int64
	endTime      int64
}

var eschedDefaultTaskPriority = 99

var eTaskPriority = map[sealtasks.TaskType]int{
	sealtasks.TTFinalize:   0,
	sealtasks.TTFetch:      1,
	sealtasks.TTCommit1:    2,
	sealtasks.TTCommit2:    3,
	sealtasks.TTPreCommit2: 4,
	sealtasks.TTPreCommit1: 5,
}

type eWorkerReqTypedList struct {
	taskType sealtasks.TaskType
	tasks    []*eWorkerRequest
}

type eWorkerReqPriorityList struct {
	priority        int
	typedTasksQueue map[sealtasks.TaskType]*eWorkerReqTypedList
}

type eWorkerHandle struct {
	priv               interface{}
	wid                WorkerID
	w                  Worker
	wt                 *workTracker
	info               storiface.WorkerInfo
	memUsed            uint64
	cpuUsed            int
	gpuUsed            int
	diskUsed           int64
	diskTotal          int64
	priorityTasksQueue map[int]*eWorkerReqPriorityList
	preparedTasks      []*eWorkerRequest
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
	taskCleanerQueue   *eWorkerRequestCleanerQueue
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

type eWorkerCleanerParam struct {
	stage        string
	sectorNumber abi.SectorNumber
	taskType     sealtasks.TaskType
}

var eschedTaskBindCleaner = map[sealtasks.TaskType]eWorkerCleanerParam{
	sealtasks.TTPreCommit2: {
		stage:    eschedWorkerCleanAtPrepare,
		taskType: sealtasks.TTPreCommit1,
	},
	sealtasks.TTFinalize: {
		stage:    eschedWorkerCleanAtFinish,
		taskType: sealtasks.TTPreCommit2,
	},
}

type eWorkerRequestCleaner struct {
	req *eWorkerRequest
	wid WorkerID
}

type eWorkerRequestCleanerQueue struct {
	mutex sync.Mutex
	queue map[abi.SectorNumber]map[sealtasks.TaskType]*eWorkerRequestCleaner
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
	taskCleanerQueue *eWorkerRequestCleanerQueue
}

const eschedWorkerBuckets = 10

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
	log.Debugf("<%s> %d %v tasks waiting for run", eschedTag, len(reqs), taskType)

	peekReqs := 0
	remainReqs := make([]*eWorkerRequest, 0)
	res := findTaskResource(bucket.spt, taskType)

	for {
		if 0 == len(reqs) {
			break
		}

		req := reqs[0]

		bindWorker := false
		bucket.taskWorkerBinder.mutex.Lock()
		address, ok := bucket.taskWorkerBinder.binder[req.sector.Number]
		if ok {
			if address != worker.info.Address {
				bucket.taskWorkerBinder.mutex.Unlock()
				remainReqs = append(remainReqs, reqs[0])
				reqs = reqs[1:]
				continue
			}
			bindWorker = true
		}
		bucket.taskWorkerBinder.mutex.Unlock()

		tasksQueue.tasks = append(tasksQueue.tasks, req)

		req.diskUsed = res.DiskSpace
		if bindWorker {
			req.diskUsed += res.InheritDiskSpace
		}

		req.inqueueTime = time.Now().UnixNano()
		worker.acquireRequestResource(req, eschedResStagePrepare)

		peekReqs += 1
		reqs = reqs[1:]
	}

	reqQueue.reqs[taskType] = remainReqs

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
	log.Infof("<%s> preparing typed task %v/%v", eschedTag, task.sector, task.taskType)
	worker.dumpWorkerInfo()
	task.dumpWorkerRequest()

	task.startTime = time.Now().UnixNano()
	err := task.prepare(task.ctx, worker.wt.worker(worker.w))
	if nil != err {
		bucket.reqFinisher <- &eRequestFinisher{
			req:  task,
			resp: &workerResponse{err: err},
			wid:  worker.wid,
		}
		return
	}

	log.Infof("<%s> prepared typed task %v/%v", eschedTag, task.sector, task.taskType)
	task.preparedTime = time.Now().UnixNano()
	worker.preparedTasks = append(worker.preparedTasks, task)
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

	cleanerParam, ok := eschedTaskBindCleaner[task.taskType]
	if ok {
		if eschedWorkerCleanAtPrepare == cleanerParam.stage {
			bucket.taskCleaner <- eWorkerCleanerParam{
				sectorNumber: task.sector.Number,
				taskType:     cleanerParam.taskType,
			}
		}
	}
}

func (bucket *eWorkerBucket) runTypedTask(worker *eWorkerHandle, task *eWorkerRequest) {
	log.Infof("<%s> executing typed task %v/%v", eschedTag, task.sector, task.taskType)
	err := task.work(task.ctx, worker.wt.worker(worker.w))
	bucket.reqFinisher <- &eRequestFinisher{
		req:  task,
		resp: &workerResponse{err: err},
		wid:  worker.wid,
	}
	task.endTime = time.Now().UnixNano()
	log.Infof("<%s> finished typed task %v/%v [%v]", eschedTag, task.sector, task.taskType, err)
	if nil != err {
		needCleaner := false
		for taskType, _ := range eschedTaskBindCleaner {
			if taskType == task.taskType {
				needCleaner = true
			}
		}

		if needCleaner {
			bucket.taskCleanerQueue.mutex.Lock()
			if _, ok := bucket.taskCleanerQueue.queue[task.sector.Number]; !ok {
				bucket.taskCleanerQueue.queue[task.sector.Number] = make(map[sealtasks.TaskType]*eWorkerRequestCleaner)
			}
			bucket.taskCleanerQueue.queue[task.sector.Number][task.taskType] = &eWorkerRequestCleaner{
				req: task,
				wid: worker.wid,
			}
			bucket.taskCleanerQueue.mutex.Unlock()
		}
	}

	cleanerParam, ok := eschedTaskBindCleaner[task.taskType]
	if ok {
		if eschedWorkerCleanAtFinish == cleanerParam.stage {
			bucket.taskCleaner <- eWorkerCleanerParam{
				sectorNumber: task.sector.Number,
				taskType:     cleanerParam.taskType,
			}
		}
	}
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
	idleCpus := int(worker.info.Resources.CPUs * 8 / 100)
	for {
		if 0 == len(worker.preparedTasks) {
			break
		}

		task := worker.preparedTasks[0]
		taskType := task.taskType
		res := findTaskResource(bucket.spt, taskType)

		needCPUs := res.CPUs
		if 0 < res.GPUs {
			if 0 < len(worker.info.Resources.GPUs) {
				if len(worker.info.Resources.GPUs) < res.GPUs+worker.gpuUsed {
					log.Infof("<%s> need %d = %d + %d GPUs but only %d available [%v]",
						eschedTag, res.GPUs+worker.gpuUsed, res.GPUs, worker.gpuUsed, taskType)
					break
				}
			} else {
				needCPUs = int(worker.info.Resources.CPUs) - idleCpus
			}
		}
		if int(worker.info.Resources.CPUs)-idleCpus < needCPUs+worker.cpuUsed {
			log.Infof("<%s> need %d = %d + %d CPUs but only %d available [%v]",
				eschedTag, needCPUs+worker.cpuUsed, needCPUs, worker.cpuUsed,
				int(worker.info.Resources.CPUs)-idleCpus, taskType)
			break
		}
		if worker.info.Resources.MemPhysical < res.Memory+worker.memUsed {
			log.Infof("<%s> need %d = %d + %d memory but only %d available [%v]",
				eschedTag, res.Memory+worker.memUsed, res.Memory, worker.memUsed,
				worker.info.Resources.MemPhysical, taskType)
			break
		}

		task.cpuUsed = needCPUs
		if 0 < len(worker.info.Resources.GPUs) {
			task.gpuUsed = res.GPUs
		}
		task.memUsed = res.Memory
		worker.acquireRequestResource(task, eschedResStageRuntime)
		worker.preparedTasks = worker.preparedTasks[1:]

		go bucket.runTypedTask(worker, task)
	}
}

func (bucket *eWorkerBucket) scheduleBucketTask() {
	log.Infof("<%s> try schedule bucket task for bucket [%d]", eschedTag, bucket.id)
	for _, worker := range bucket.workers {
		bucket.scheduleTypedTasks(worker)
	}
}

func (bucket *eWorkerBucket) schedulePreparedTask() {
	log.Infof("<%s> try schedule prepared task for bucket [%d]", eschedTag, bucket.id)
	for _, worker := range bucket.workers {
		bucket.schedulePreparedTasks(worker)
	}
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
				for _, task := range tq.tasks {
					go func() { bucket.retRequest <- task }()
				}
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
	bucket.taskCleanerQueue.mutex.Lock()
	if sectorCleaner, ok := bucket.taskCleanerQueue.queue[param.sectorNumber]; ok {
		if cleaner, ok := sectorCleaner[param.taskType]; ok {
			worker := bucket.findBucketWorkerByID(cleaner.wid)
			if nil != worker {
				worker.releaseRequestResource(cleaner.req, eschedResStagePrepare)
			}
			delete(sectorCleaner, param.taskType)
		}
		if 0 == len(sectorCleaner) {
			delete(bucket.taskCleanerQueue.queue, param.sectorNumber)
		}
	}
	bucket.taskCleanerQueue.mutex.Unlock()
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
		}
	}

	log.Infof("<%s> finish scheduler for bucket %d", eschedTag, bucket.id)
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
		taskCleanerQueue: &eWorkerRequestCleanerQueue{
			queue: make(map[abi.SectorNumber]map[sealtasks.TaskType]*eWorkerRequestCleaner),
		},
	}

	for i := range dispatcher.buckets {
		dispatcher.buckets[i] = &eWorkerBucket{
			spt:                spt,
			id:                 i,
			newWorker:          make(chan *eWorkerHandle),
			workers:            make([]*eWorkerHandle, 0),
			reqQueue:           dispatcher.reqQueue,
			schedulerWaker:     make(chan struct{}, 20),
			schedulerRunner:    make(chan struct{}, 20),
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
			taskCleanerQueue:   dispatcher.taskCleanerQueue,
		}
		go dispatcher.buckets[i].scheduler()
	}

	eResourceTable[sealtasks.TTUnseal] = eResourceTable[sealtasks.TTPreCommit1]
	eResourceTable[sealtasks.TTFinalize] = eResourceTable[sealtasks.TTCommit1]
	eResourceTable[sealtasks.TTFetch] = eResourceTable[sealtasks.TTCommit1]
	eResourceTable[sealtasks.TTReadUnsealed] = eResourceTable[sealtasks.TTCommit1]

	return dispatcher
}

func (sh *edispatcher) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.newRequest <- &eWorkerRequest{
		sector:   sector,
		taskType: taskType,
		prepare:  prepare,
		work:     work,
		ret:      ret,
		ctx:      ctx,
	}:
	}

	select {
	case resp := <-ret:
		return resp.err
	}

	return nil
}

func (w *eWorkerHandle) dumpWorkerInfo() {
	log.Debugf("Worker information -----------")
	log.Debugf("  Address: ------------------- %v", w.info.Address)
	log.Debugf("  Group: --------------------- %v", w.info.GroupName)
	log.Debugf("  Support Tasks: -------------")
	for _, taskType := range w.info.SupportTasks {
		log.Debugf("    + %v", taskType)
	}
	log.Debugf("  Hostname: ------------------ %v", w.info.Hostname)
	log.Debugf("  Mem Physical: -------------- %v", w.info.Resources.MemPhysical)
	log.Debugf("  Mem Used: ------------------ %v", w.memUsed)
	log.Debugf("  Mem Swap: ------------------ %v", w.info.Resources.MemSwap)
	log.Debugf("  Mem Reserved: -------------- %v", w.info.Resources.MemReserved)
	log.Debugf("  CPUs: ---------------------- %v", w.info.Resources.CPUs)
	log.Debugf("  CPU Used: ------------------ %v", w.cpuUsed)
	log.Debugf("  GPUs------------------------")
	for _, gpu := range w.info.Resources.GPUs {
		log.Debugf("    + %s", gpu)
	}
	log.Debugf("  GPU Used: ------------------ %v", w.gpuUsed)
	log.Debugf("  Disk Used: ----------------- %v", w.diskUsed)
}

func (w *eWorkerHandle) patchLocalhost() {
	if w.info.Address == "" {
		w.info.Address = "localhost"
		w.info.Hostname = "localhost"
		w.info.GroupName = "localhost"
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

	w.preparedTasks = make([]*eWorkerRequest, 0)

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
	// TODO: close scheduler of buckets and cleanup all workers in scheduler
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
