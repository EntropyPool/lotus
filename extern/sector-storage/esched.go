package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	"math/rand"
	"os"
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
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 8 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: 64 * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 4 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: 32 * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 512 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 512 * eMiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * eKiB, CPUs: 1, GPUs: 0, DiskSpace: 2 * eKiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 8 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: 8 * eMiB * 11 / 10},
	},
	sealtasks.TTPreCommit1: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 128 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: (64*15 + 80) * eGiB, DisableSwap: true},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 64 * eGiB, CPUs: 1, GPUs: 0, DiskSpace: (32*15 + 40) * eGiB, DisableSwap: true},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: eGiB, CPUs: 1, GPUs: 0, DiskSpace: (512*15 + 512) * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * eKiB, CPUs: 1, GPUs: 0, DiskSpace: (2*15 + 2) * eKiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 16 * eMiB, CPUs: 1, GPUs: 0, DiskSpace: (8*15 + 8) * eMiB},
	},
	sealtasks.TTPreCommit2: {
		/* Specially, for P2 at the different worker as PC1, it should add disk space of PC1 */
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 32 * eGiB, CPUs: 0, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: (64*15 + 80) * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 32 * eGiB, CPUs: 0, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: (32*15 + 40) * eGiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: eGiB, CPUs: 0, GPUs: 1, DiskSpace: 512 * eMiB, InheritDiskSpace: (512*15 + 1) * eMiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * eKiB, CPUs: 0, GPUs: 0, DiskSpace: 2 * eMiB, InheritDiskSpace: (2*15 + 1) * eKiB * 11 / 10},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 16 * eMiB, CPUs: 0, GPUs: 0, DiskSpace: 16 * eMiB, InheritDiskSpace: (8*15 + 1) * eMiB * 11 / 10},
	},
	sealtasks.TTCommit1: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 1 * eGiB, CPUs: 0, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 1 * eGiB, CPUs: 0, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 32 * eMiB, CPUs: 0, GPUs: 0, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 1 * eKiB, CPUs: 0, GPUs: 0, DiskSpace: 2 * eMiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 1 * eMiB, CPUs: 0, GPUs: 0, DiskSpace: 16 * eMiB},
	},
	sealtasks.TTCommit2: {
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 410 * eGiB, CPUs: 0, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 240 * eGiB, CPUs: 0, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 16 * eGiB, CPUs: 0, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 32 * eKiB, CPUs: 0, GPUs: 0, DiskSpace: 2 * eMiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 32 * eMiB, CPUs: 0, GPUs: 0, DiskSpace: 16 * eMiB},
	},
}

type eWorkerRequest struct {
	id              uint64
	uuid            uuid.UUID
	sector          storage.SectorRef
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
	deadTime        int64
	endTime         int64
	startRunTimeRaw time.Time
	sel             WorkerSelector
	priority        int
}

var eTaskPriority = map[sealtasks.TaskType]int{
	sealtasks.TTFinalize:     1,
	sealtasks.TTFetch:        2,
	sealtasks.TTCommit1:      3,
	sealtasks.TTCommit2:      4,
	sealtasks.TTPreCommit2:   5,
	sealtasks.TTPreCommit1:   6,
	sealtasks.TTAddPiece:     7,
	sealtasks.TTReadUnsealed: 8,
	sealtasks.TTUnseal:       9,
}

var eSealProofType = []abi.RegisteredSealProof{
	abi.RegisteredSealProof_StackedDrg64GiBV1_1,
	abi.RegisteredSealProof_StackedDrg32GiBV1_1,
	abi.RegisteredSealProof_StackedDrg512MiBV1_1,
	abi.RegisteredSealProof_StackedDrg8MiBV1_1,
	abi.RegisteredSealProof_StackedDrg2KiBV1_1,
	abi.RegisteredSealProof_StackedDrg64GiBV1,
	abi.RegisteredSealProof_StackedDrg32GiBV1,
	abi.RegisteredSealProof_StackedDrg512MiBV1,
	abi.RegisteredSealProof_StackedDrg8MiBV1,
	abi.RegisteredSealProof_StackedDrg2KiBV1,
}

var eTaskTimeout = map[sealtasks.TaskType]int64{
	sealtasks.TTPreCommit1:   18 * 60 * 60 * 1000000000,
	sealtasks.TTPreCommit2:   18 * 60 * 60 * 1000000000,
	sealtasks.TTFinalize:     5 * 60 * 60 * 1000000000,
	sealtasks.TTFetch:        5 * 60 * 60 * 1000000000,
	sealtasks.TTCommit1:      1 * 60 * 60 * 1000000000,
	sealtasks.TTCommit2:      2 * 60 * 60 * 1000000000,
	sealtasks.TTReadUnsealed: 10 * 60 * 1000000000,
	sealtasks.TTUnseal:       10 * 60 * 1000000000,
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
	wid uuid.UUID
	req *eWorkerRequest
}

type eWorkerTaskCleaning struct {
	sector         storage.SectorRef
	taskType       sealtasks.TaskType
	byCacheMove    bool
	bySectorRemove bool
}

type eWorkerSyncTaskList struct {
	mutex sync.Mutex
	queue []*eWorkerRequest // may better to be priority queue
}

const eschedWorkerStateWaving = "waving"
const eschedWorkerStateReady = "ready"

type eStoreStat struct {
	total      int64
	space      int64
	reserved   int64
	URLs       []string
	local      bool
	id         stores.ID
	maxReached bool
}

type eWorkerHandle struct {
	wid                   uuid.UUID
	wIndex                uint64
	w                     Worker
	wt                    *workTracker
	info                  storiface.WorkerInfo
	maintaining           bool
	rejectNewTask         bool
	memUsed               uint64
	cpuUsed               int
	gpuUsed               int
	diskUsed              int64
	hugePageBytes         uint64
	hugePageUsed          uint64
	state                 string
	priorityTasksQueue    []*eWorkerReqPriorityList
	preparedTasks         *eWorkerSyncTaskList
	preparingTasks        *eWorkerSyncTaskList
	runningTasks          []*eWorkerRequest
	cleaningTasks         []*eWorkerTaskCleaning
	maxConcurrent         map[abi.RegisteredSealProof]map[sealtasks.TaskType]int
	maxRuntimeConcurrent  map[abi.RegisteredSealProof]int
	memoryConcurrentLimit map[abi.RegisteredSealProof]int
	diskConcurrentLimit   map[abi.RegisteredSealProof]int
	storeIDs              map[stores.ID]eStoreStat
}

const eschedTag = "esched"

type eRequestFinisher struct {
	resp *workerResponse
	req  *eWorkerRequest
	wid  uuid.UUID
}

type eTaskWorkerBindParam struct {
	address string
	wid     uuid.UUID
}

type eTaskWorkerBinder struct {
	mutex  sync.Mutex
	binder map[abi.SectorNumber]eTaskWorkerBindParam
}

type eTaskUUID struct {
	sector storage.SectorRef
	uuid   uuid.UUID
}

type eWorkerMode struct {
	address string
	mode    string
}

type eWorkerReservedSpace struct {
	address     string
	storePrefix string
	reserved    int64
}

type eWorkerBucket struct {
	spt                    abi.RegisteredSealProof
	id                     int
	idleCpus               int
	usableCpus             int
	gpuTasks               int
	concurrentAP           int
	newWorker              chan *eWorkerHandle
	workers                []*eWorkerHandle
	reqQueue               *eRequestQueue
	schedulerWaker         chan struct{}
	schedulerRunner        chan struct{}
	reqFinisher            chan *eRequestFinisher
	notifier               chan struct{}
	dropWorker             chan uuid.UUID
	abortTask              chan storage.SectorRef
	taskUUID               chan eTaskUUID
	retRequest             chan *eWorkerRequest
	storageNotifier        chan eStoreAction
	droppedWorker          chan eTaskWorkerBindParam
	taskWorkerBinder       *eTaskWorkerBinder
	taskCleaner            chan *eWorkerTaskCleaning
	taskCleanerHandler     chan *eWorkerTaskCleaning
	workerStatsQuery       chan *eWorkerStatsParam
	workerJobsQuery        chan *eWorkerJobsParam
	bucketPledgedJobs      chan *eBucketPledgedJobsParam
	setWorkerMode          chan eWorkerMode
	setWorkerReservedSpace chan eWorkerReservedSpace
	closing                chan struct{}
	ticker                 *time.Ticker
}

type eRequestQueue struct {
	reqs  map[sealtasks.TaskType][]*eWorkerRequest
	mutex sync.Mutex
}

const eschedAdd = "add"
const eschedDrop = "drop"

const eschedResStagePrepare = "prepare"
const eschedResStageRuntime = "runtime"

const eschedLeastWaveKeep = 60 * 10 * time.Second

type eWorkerAction struct {
	address string
	act     string
}

const eschedStartInterval = 100 * time.Second
const eschedEndInterval = 800 * time.Second

type EStorage struct {
	ctx                 context.Context
	index               stores.SectorIndex
	indexInstance       *stores.Index
	ls                  *stores.Local
	storageChecker      *time.Ticker
	lastCheckerInterval time.Duration
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

type eWorkerCleanerAttr struct {
	stage    string
	taskType sealtasks.TaskType
}

var eschedTaskCleanMap = map[sealtasks.TaskType]*eWorkerCleanerAttr{
	/**
	 taskTypeToRunClean: &attr {
		 cleanStage: prepare or finish,
		 cleanedTaskType
	 }
	*/
	sealtasks.TTPreCommit2: &eWorkerCleanerAttr{
		stage:    eschedWorkerCleanAtPrepare,
		taskType: sealtasks.TTPreCommit1,
	},
	sealtasks.TTCommit2: &eWorkerCleanerAttr{
		stage:    eschedWorkerCleanAtFinish,
		taskType: sealtasks.TTPreCommit2,
	},
}

var eschedTaskHugePage = map[sealtasks.TaskType]bool{
	sealtasks.TTPreCommit1: true,
}

var eschedTaskRuntimeLimitHalf = map[sealtasks.TaskType][]sealtasks.TaskType{
	sealtasks.TTPreCommit1: []sealtasks.TaskType{sealtasks.TTCommit2 /*, sealtasks.TTPreCommit2*/},
	// sealtasks.TTAddPiece:   []sealtasks.TaskType{sealtasks.TTCommit2, sealtasks.TTPreCommit2, sealtasks.TTPreCommit1},
}

var eschedTaskLimitMerge = map[sealtasks.TaskType][]sealtasks.TaskType{
	sealtasks.TTAddPiece:   []sealtasks.TaskType{sealtasks.TTPreCommit1, sealtasks.TTPreCommit2},
	sealtasks.TTPreCommit1: []sealtasks.TaskType{sealtasks.TTAddPiece, sealtasks.TTPreCommit2},
	// sealtasks.TTPreCommit2: []sealtasks.TaskType{sealtasks.TTAddPiece, sealtasks.TTPreCommit1},
}

var eschedTaskMaintainingReject = map[sealtasks.TaskType]bool{
	sealtasks.TTAddPiece: true,
	// sealtasks.TTPreCommit1: true,
}

var eschedTaskPeekByRuntimeLimit = map[sealtasks.TaskType]bool{
	sealtasks.TTAddPiece:   true,
	sealtasks.TTPreCommit1: true,
	sealtasks.TTPreCommit2: true,
}

var eschedTaskStableRunning = map[sealtasks.TaskType]struct{}{
	sealtasks.TTAddPiece:   struct{}{},
	sealtasks.TTPreCommit1: struct{}{},
	sealtasks.TTPreCommit2: struct{}{},
}

var eschedTaskSingleRunning = map[sealtasks.TaskType][]sealtasks.TaskType{
	sealtasks.TTPreCommit2: {sealtasks.TTCommit2},
	// sealtasks.TTCommit2:    {sealtasks.TTPreCommit2},
}

const eschedWorkerJobs = "worker_jobs"
const eschedWorkerStats = "worker_stats"

type eWorkerStatsParam struct {
	command string
	resp    chan map[uuid.UUID]storiface.WorkerStats
}

type eBucketPledgedJobsParam struct {
	resp chan int
}

type eWorkerJobsParam struct {
	command string
	resp    chan map[uuid.UUID][]storiface.WorkerJob
}

type edispatcher struct {
	nextRequest            uint64
	nextWorker             uint64
	newWorker              chan *eWorkerHandle
	dropWorker             chan uuid.UUID
	taskUUID               chan eTaskUUID
	abortTask              chan storage.SectorRef
	setWorkerMode          chan eWorkerMode
	setWorkerReservedSpace chan eWorkerReservedSpace
	newRequest             chan *eWorkerRequest
	buckets                []*eWorkerBucket
	reqQueue               *eRequestQueue
	storage                *EStorage
	storageNotifier        chan eStoreAction
	droppedWorker          chan eTaskWorkerBindParam
	taskCleaner            chan *eWorkerTaskCleaning
	closing                chan struct{}
	ctx                    context.Context
	taskWorkerBinder       *eTaskWorkerBinder
	workerStatsQuery       chan *eWorkerStatsParam
	workerJobsQuery        chan *eWorkerJobsParam
	bucketPledgedJobs      chan *eBucketPledgedJobsParam
	workerJobs             map[uuid.UUID][]storiface.WorkerJob
	workerJobsResp         chan map[uuid.UUID][]storiface.WorkerJob
	workerStats            map[uuid.UUID]storiface.WorkerStats
	workerStatsResp        chan map[uuid.UUID]storiface.WorkerStats
	statsTicker            *time.Ticker
	state                  string
}

const eschedWorkerBuckets = 10

var eschedUnassignedWorker = uuid.Must(uuid.Parse("11111111-2222-3333-4444-111111111111"))
var eschedDebug = false

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
	go func() {
		sh.storageNotifier <- eStoreAction{
			id:   id,
			act:  act,
			stat: stat,
		}
	}()
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
			sh.storeNotify(id, eStoreStat{}, eschedDrop)
			continue
		}

		if err := stor.HeartbeatError(); nil != err {
			log.Errorf("<%s> delete storage %v to watcher [%v | %v]", eschedTag, id, timeout, err)
			sh.dumpStorageInfo(stor)
			sh.storeNotify(id, eStoreStat{}, eschedDrop)
			continue
		}

		stat := eStoreStat{
			total: stor.FsStat().Capacity,
			space: stor.FsStat().Available,
			URLs:  stor.Info().URLs,
			local: sh.isLocalStorage(id),
			id:    id,
		}

		sh.storeNotify(id, stat, eschedAdd)
	}
}

func (sh *edispatcher) updateStorageChecker() {
	if eschedEndInterval <= sh.storage.lastCheckerInterval {
		return
	}
	sh.storage.lastCheckerInterval *= 2
	sh.storage.storageChecker.Stop()
	sh.storage.storageChecker = time.NewTicker(sh.storage.lastCheckerInterval)
}

func (sh *edispatcher) storageWatcher() {
	for {
		select {
		case <-sh.storage.storageChecker.C:
			sh.checkStorageUpdate()
			sh.updateStorageChecker()
		case <-sh.storage.indexInstance.StorageNotifier:
			sh.checkStorageUpdate()
		}
	}
}

func (sh *edispatcher) SetStorage(storage *EStorage) {
	sh.storage = storage

	value := reflect.ValueOf(sh.storage.index)
	sh.storage.indexInstance = value.Interface().(*stores.Index)
	sh.storage.storageChecker = time.NewTicker(eschedStartInterval)
	sh.storage.lastCheckerInterval = eschedStartInterval

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
		hugepage, ok := eschedTaskHugePage[req.taskType]
		if ok && hugepage && 0 < w.hugePageBytes {
			w.hugePageUsed += req.memUsed
		} else {
			w.memUsed += req.memUsed
		}
		log.Infof("<%s> acquire runtime resource %v / %v, gpu %v, cpu %v [%v], mem %v / %v / %v [%v]",
			eschedTag, req.sector, req.taskType, req.gpuUsed, req.cpuUsed, w.info.Address,
			hugepage, w.hugePageUsed, w.memUsed, req.memUsed)
	}
}

func (w *eWorkerHandle) releaseRequestResource(req *eWorkerRequest, resType string) {
	switch resType {
	case eschedResStagePrepare:
		w.diskUsed -= req.diskUsed
	case eschedResStageRuntime:
		w.cpuUsed -= req.cpuUsed
		w.gpuUsed -= req.gpuUsed
		hugepage, ok := eschedTaskHugePage[req.taskType]
		if ok && hugepage && 0 < w.hugePageBytes {
			w.hugePageUsed -= req.memUsed
		} else {
			w.memUsed -= req.memUsed
		}
		log.Infof("<%s> release runtime resource %v / %v, gpu %v, cpu %v [%v], mem %v / %v / %v [%v]",
			eschedTag, req.sector, req.taskType, req.gpuUsed, req.cpuUsed, w.info.Address,
			hugepage, w.hugePageUsed, w.memUsed, req.memUsed)
	}
}

func safeRemoveWorkerRequest(slice []*eWorkerRequest, accepter []*eWorkerRequest, req *eWorkerRequest) ([]*eWorkerRequest, []*eWorkerRequest) {
	if 0 == len(slice) {
		return slice, accepter
	}
	if nil != accepter {
		accepter = append(accepter, req)
	}

	pos := -1
	for i, task := range slice {
		if req == task && req.taskType == task.taskType && req.sector.ID.Number == task.sector.ID.Number {
			pos = i
			break
		}
	}

	if 0 <= pos {
		queue := make([]*eWorkerRequest, 0)
		queue = append(queue, slice[0:pos]...)
		queue = append(queue, slice[pos+1:]...)
		slice = queue
	}

	return slice, accepter
}

func (worker *eWorkerHandle) taskCleaning(task *eWorkerRequest) bool {
	for _, req := range worker.cleaningTasks {
		if task.sector.ID.Number == req.sector.ID.Number {
			return true
		}
	}
	return false
}

func (worker *eWorkerHandle) typedTaskCount(taskType sealtasks.TaskType, includeCleaning bool) int {
	taskCount := 0
	worker.preparingTasks.mutex.Lock()
	for _, task := range worker.preparingTasks.queue {
		if task.taskType == taskType && !worker.taskCleaning(task) {
			taskCount += 1
		}
	}
	worker.preparingTasks.mutex.Unlock()

	worker.preparedTasks.mutex.Lock()
	for _, task := range worker.preparedTasks.queue {
		if task.taskType == taskType && !worker.taskCleaning(task) {
			taskCount += 1
		}
	}
	worker.preparedTasks.mutex.Unlock()

	for _, task := range worker.runningTasks {
		if task.taskType == taskType && !worker.taskCleaning(task) {
			taskCount += 1
		}
	}

	if includeCleaning {
		for _, task := range worker.cleaningTasks {
			if task.taskType == taskType {
				taskCount += 1
			}
		}
	}

	for _, pq := range worker.priorityTasksQueue {
		for _, tq := range pq.typedTasksQueue {
			for _, task := range tq.tasks {
				if task.taskType == taskType && !worker.taskCleaning(task) {
					taskCount += 1
				}
			}
		}
	}

	return taskCount
}

func (bucket *eWorkerBucket) tryPeekAsManyRequests(worker *eWorkerHandle, taskType sealtasks.TaskType, tasksQueue *eWorkerReqTypedList, reqQueue *eRequestQueue) int {
	log.Debugf("<%s> %d %v tasks waiting for run [%s]", eschedTag, len(reqQueue.reqs[taskType]), taskType, worker.info.Address)

	peekReqs := 0

	reqs := reqQueue.reqs[taskType]
	remainReqs := make([]*eWorkerRequest, 0)

	for {
		if 0 == len(reqs) {
			break
		}

		req := reqs[0]
		bucket.spt = req.sector.ProofType
		_, ok := eschedTaskStableRunning[taskType]

		if 0 == worker.diskConcurrentLimit[req.sector.ProofType] && ok {
			log.Debugf("<%s> worker %s's disk concurrent limit is not set, %v need to run at stable state",
				eschedTag, worker.info.Address, taskType)
			return 0
		}

		taskCount := worker.typedTaskCount(req.taskType, true)
		if taskTypes, ok := eschedTaskLimitMerge[req.taskType]; ok {
			for _, lTaskType := range taskTypes {
				taskCount += worker.typedTaskCount(lTaskType, false)
			}
		}

		curConcurrentLimit := worker.maxConcurrent[req.sector.ProofType]
		taskRuntimeLimit := curConcurrentLimit[req.taskType]

		if byRuntimeLimit, ok := eschedTaskPeekByRuntimeLimit[req.taskType]; ok && byRuntimeLimit {
			taskRuntimeLimit = worker.maxRuntimeConcurrent[req.sector.ProofType]
		}

		if taskRuntimeLimit <= taskCount {
			log.Debugf("<%s> worker %s's %v tasks queue is full %d / %d",
				eschedTag, worker.info.Address, req.taskType,
				taskCount, curConcurrentLimit[req.taskType])
			reqs, remainReqs = safeRemoveWorkerRequest(reqs, remainReqs, req)
			continue
		}

		apCount := worker.typedTaskCount(sealtasks.TTAddPiece, false)
		if req.taskType == sealtasks.TTAddPiece && bucket.concurrentAP <= apCount {
			reqs, remainReqs = safeRemoveWorkerRequest(reqs, remainReqs, req)
			continue
		}

		bucket.taskWorkerBinder.mutex.Lock()
		binder, ok := bucket.taskWorkerBinder.binder[req.sector.ID.Number]
		if ok {
			if binder.address != worker.info.Address || binder.wid != worker.wid {
				bucket.taskWorkerBinder.mutex.Unlock()
				log.Debugf("<%s> task %v/%v is binded to %s, skip by %s",
					eschedTag, req.sector.ID, req.taskType, binder.address, worker.info.Address)
				reqs, remainReqs = safeRemoveWorkerRequest(reqs, remainReqs, req)
				continue
			}
		}
		bucket.taskWorkerBinder.mutex.Unlock()

		rpcCtx, cancel := context.WithTimeout(req.ctx, SelectorTimeout)
		ok, err := req.sel.Ok(rpcCtx, req.taskType, req.sector.ProofType, worker)
		cancel()
		if err != nil {
			log.Debugf("<%s> cannot judge worker %s for task %v/%v status %v",
				eschedTag, worker.info.Address, req.sector.ID, req.taskType, err)
			reqs, remainReqs = safeRemoveWorkerRequest(reqs, remainReqs, req)
			continue
		}

		if !ok {
			log.Debugf("<%s> worker %s is not OK for task %v/%v",
				eschedTag, worker.info.Address, req.sector.ID, req.taskType)
			reqs, remainReqs = safeRemoveWorkerRequest(reqs, remainReqs, req)
			continue
		}

		req.inqueueTime = time.Now().UnixNano()
		req.inqueueTimeRaw = time.Now()
		worker.acquireRequestResource(req, eschedResStagePrepare)

		peekReqs += 1
		reqs, _ = safeRemoveWorkerRequest(reqs, nil, req)
		tasksQueue.tasks = append(tasksQueue.tasks, req)
		log.Infof("<%s> worker %s peek task %v/%v", eschedTag, worker.info.Address, req.sector.ID, req.taskType)
		taskCount += 1
	}

	reqQueue.reqs[taskType] = append(remainReqs, reqs[0:]...)

	return peekReqs
}

func (bucket *eWorkerBucket) removeObseleteTask() {
	bucket.reqQueue.mutex.Lock()
	for taskType, reqs := range bucket.reqQueue.reqs {
		if _, ok := eTaskTimeout[taskType]; !ok {
			continue
		}
		now := time.Now().UnixNano()
		remainReqs := make([]*eWorkerRequest, 0)
		for _, req := range reqs {
			if req.deadTime <= now {
				go func(req *eWorkerRequest) {
					req.ret <- workerResponse{err: xerrors.Errorf("cannot find any good worker")}
				}(req)
				continue
			}
			remainReqs = append(remainReqs, req)
		}
		bucket.reqQueue.reqs[taskType] = remainReqs
	}
	bucket.reqQueue.mutex.Unlock()
}

func (bucket *eWorkerBucket) tryPeekRequest() {
	log.Debugf("<%s> try peek schedule request from %d workers of bucket [%d]", eschedTag, len(bucket.workers), bucket.id)

	peekReqs := 0

	for _, worker := range bucket.workers {
		apCount := bucket.waitingJobs(worker, sealtasks.TTAddPiece)
		apCount += worker.typedTaskCount(sealtasks.TTAddPiece, false)

		pc1Waiting := bucket.waitingJobs(worker, sealtasks.TTPreCommit1)
		pc1Running := worker.typedTaskCount(sealtasks.TTPreCommit1, false)

		for _, pq := range worker.priorityTasksQueue {
			for taskType, tq := range pq.typedTasksQueue {
				if taskType == sealtasks.TTAddPiece {
					if 0 < pc1Waiting {
						continue
					}
					if bucket.concurrentAP <= apCount {
						continue
					}
				}

				if worker.rejectNewTask || worker.maintaining {
					if _, ok := eschedTaskMaintainingReject[taskType]; ok {
						continue
					}
				}
				bucket.reqQueue.mutex.Lock()
				if _, ok := bucket.reqQueue.reqs[taskType]; ok {
					if 0 < len(bucket.reqQueue.reqs[taskType]) {
						peekReqs += bucket.tryPeekAsManyRequests(worker, taskType, tq, bucket.reqQueue)
					}
				}
				bucket.reqQueue.mutex.Unlock()

				log.Infof("<%s> peek %v %v reqs (ap count %v, pc1 count %v (%v + %v)) [%s]",
					eschedTag, peekReqs, taskType, apCount, pc1Waiting+pc1Running,
					pc1Waiting, pc1Running, worker.info.Address)
			}
		}
	}

	if 0 < peekReqs {
		go func() { bucket.schedulerWaker <- struct{}{} }()
	}

	bucket.removeObseleteTask()
}

func (bucket *eWorkerBucket) prepareTypedTask(worker *eWorkerHandle, task *eWorkerRequest) {
	log.Debugf("<%s> preparing typed task %v/%v to %s", eschedTag, task.sector.ID, task.taskType, worker.info.Address)
	task.dumpWorkerRequest()

	task.startTime = time.Now().UnixNano()

	worker.preparingTasks.mutex.Lock()
	worker.preparingTasks.queue = append(worker.preparingTasks.queue, task)
	worker.preparingTasks.mutex.Unlock()

	err := task.prepare(task.ctx, worker.wt.worker(WorkerID(worker.wid), worker.w))

	worker.preparingTasks.mutex.Lock()
	for idx, req := range worker.preparingTasks.queue {
		if req.id == task.id {
			worker.preparingTasks.queue = append(worker.preparingTasks.queue[0:idx], worker.preparingTasks.queue[idx+1:]...)
			break
		}
	}
	worker.preparingTasks.mutex.Unlock()

	if nil != err {
		log.Errorf("<%s> cannot prepare typed task %v/%v/%s[%v]", eschedTag, task.sector.ID, task.taskType, worker.info.Address, err)
		go func() {
			bucket.reqFinisher <- &eRequestFinisher{
				req:  task,
				resp: &workerResponse{err: err},
				wid:  worker.wid,
			}
		}()
		return
	}

	log.Debugf("<%s> prepared typed task %v/%v by %s", eschedTag, task.sector.ID, task.taskType, worker.info.Address)
	task.preparedTime = time.Now().UnixNano()
	task.preparedTimeRaw = time.Now()

	worker.preparedTasks.mutex.Lock()
	priority := getTaskPriority(task.taskType)
	queue := make([]*eWorkerRequest, len(worker.preparedTasks.queue)+1)

	idx := 0
	inserted := false

	for _, req := range worker.preparedTasks.queue {
		lPriority := getTaskPriority(req.taskType)
		if priority < lPriority && !inserted {
			queue[idx] = task
			idx += 1
			inserted = true
		}
		queue[idx] = req
		idx += 1
	}

	if !inserted {
		queue[idx] = task
	}

	worker.preparedTasks.queue = queue
	worker.preparedTasks.mutex.Unlock()

	go func() { bucket.schedulerRunner <- struct{}{} }()

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
		bucket.taskWorkerBinder.binder[task.sector.ID.Number] =
			eTaskWorkerBindParam{address: worker.info.Address, wid: worker.wid}
	} else {
		delete(bucket.taskWorkerBinder.binder, task.sector.ID.Number)
	}
	bucket.taskWorkerBinder.mutex.Unlock()

	bucket.doCleanTask(task, eschedWorkerCleanAtPrepare)
}

func (bucket *eWorkerBucket) addCleaningTask(wid uuid.UUID, task *eWorkerRequest) {
	worker := bucket.findBucketWorkerByID(wid)

	if nil == worker {
		return
	}

	if worker.taskCleaning(task) {
		return
	}

	for _, attr := range eschedTaskCleanMap {
		if task.taskType == attr.taskType {
			log.Infof("<%s> add task %v / %v to [%v] cleaning list of %s",
				eschedTag, task.sector.ID,
				task.taskType, attr.taskType,
				worker.info.Address)
			worker.cleaningTasks = append(worker.cleaningTasks, &eWorkerTaskCleaning{
				sector:   task.sector,
				taskType: task.taskType,
			})
		}
	}
}

func (bucket *eWorkerBucket) doCleanTask(task *eWorkerRequest, stage string) {
	clean, ok := eschedTaskCleanMap[task.taskType]
	if !ok {
		return
	}

	if clean.stage != stage {
		return
	}

	go func() {
		bucket.taskCleaner <- &eWorkerTaskCleaning{
			sector:   task.sector,
			taskType: clean.taskType,
		}
	}()
}

func (bucket *eWorkerBucket) runTypedTask(worker *eWorkerHandle, task *eWorkerRequest) {
	log.Debugf("<%s> executing typed task %v/%v at %s", eschedTag, task.sector.ID, task.taskType, worker.info.Address)
	task.startRunTimeRaw = time.Now()
	err := task.work(task.ctx, worker.wt.worker(WorkerID(worker.wid), worker.w))
	task.endTime = time.Now().UnixNano()
	log.Infof("<%s> finished typed task %v/%v duration (%dms / %d ms / %d ms) by %s [%v]",
		eschedTag, task.sector.ID, task.taskType,
		(task.inqueueTime-task.requestTime)/1000000.0,
		(task.preparedTime-task.inqueueTime)/1000000.0,
		(task.endTime-task.preparedTime)/1000000.0,
		worker.info.Address, err)

	go func() {
		bucket.reqFinisher <- &eRequestFinisher{
			req:  task,
			resp: &workerResponse{err: err},
			wid:  worker.wid,
		}
	}()

	if nil == err {
		bucket.doCleanTask(task, eschedWorkerCleanAtFinish)
	}
}

func (bucket *eWorkerBucket) scheduleTypedTasks(worker *eWorkerHandle) bool {
	scheduled := false
	for _, pq := range worker.priorityTasksQueue {
		for _, typedTasks := range pq.typedTasksQueue {
			tasks := typedTasks.tasks
			for {
				if 0 == len(tasks) {
					break
				}
				task := tasks[0]
				go bucket.prepareTypedTask(worker, task)
				tasks, _ = safeRemoveWorkerRequest(tasks, nil, task)
				scheduled = true
			}
			typedTasks.tasks = tasks
		}
		if scheduled {
			// Always run only one priority for each time
			return true
		}
	}
	return false
}

func (worker *eWorkerHandle) typedRunningTasks(taskType sealtasks.TaskType) int {
	running := 0

	for _, task := range worker.runningTasks {
		if task.taskType == taskType {
			running += 1
		}
	}
	return running
}

func (bucket *eWorkerBucket) schedulePreparedTasks(worker *eWorkerHandle) {
	idleCpus := bucket.idleCpus
	if worker.info.Resources.CPUs <= uint64(idleCpus) {
		idleCpus = 1
	}

	remainReqs := make([]*eWorkerRequest, 0)

	for {
		worker.preparedTasks.mutex.Lock()
		if 0 == len(worker.preparedTasks.queue) {
			worker.preparedTasks.mutex.Unlock()
			break
		}
		task := worker.preparedTasks.queue[0]
		worker.preparedTasks.mutex.Unlock()

		halfTasks := 0
		if taskTypes, ok := eschedTaskRuntimeLimitHalf[task.taskType]; ok {
			for _, lTaskType := range taskTypes {
				halfTasks += worker.typedTaskCount(lTaskType, false)
			}
		}

		maxRuntimeConcurrentTasks := int(worker.info.Resources.CPUs - uint64(idleCpus))
		if 0 < bucket.usableCpus {
			maxRuntimeConcurrentTasks = bucket.usableCpus
		}
		if 0 < halfTasks {
			runningTasks := int(worker.info.Resources.CPUs / 2)
			if maxRuntimeConcurrentTasks < runningTasks {
				runningTasks = maxRuntimeConcurrentTasks
			}
			idleCpus = int(worker.info.Resources.CPUs - uint64(runningTasks))
		}

		gpuTasks := len(worker.info.Resources.GPUs)
		if 0 < bucket.gpuTasks {
			gpuTasks = bucket.gpuTasks
		}

		if _, ok := eschedTaskSingleRunning[task.taskType]; ok {
			gpuTasks = 1
		}

		taskType := task.taskType
		res := findTaskResource(task.sector.ProofType, taskType)

		if 0 < res.GPUs && gpuTasks < len(worker.info.Resources.GPUs) {
			runningTasks := worker.typedRunningTasks(task.taskType)
			if gpuTasks <= runningTasks {
				worker.preparedTasks.mutex.Lock()
				worker.preparedTasks.queue, _ = safeRemoveWorkerRequest(worker.preparedTasks.queue, nil, task)
				worker.preparedTasks.mutex.Unlock()
				remainReqs = append(remainReqs, task)
				continue
			}
		}

		needCPUs := res.CPUs
		if 0 < res.GPUs {
			if 0 < len(worker.info.Resources.GPUs) {
				if len(worker.info.Resources.GPUs) < res.GPUs+worker.gpuUsed {
					log.Infof("<%s> need %d = %d + %d GPUs but only %d available [%v / %v] [%s]",
						eschedTag, res.GPUs+worker.gpuUsed, res.GPUs, worker.gpuUsed,
						len(worker.info.Resources.GPUs)-worker.gpuUsed, task.sector.ID,
						taskType, worker.info.Address)
					worker.preparedTasks.mutex.Lock()
					worker.preparedTasks.queue, _ = safeRemoveWorkerRequest(worker.preparedTasks.queue, nil, task)
					worker.preparedTasks.mutex.Unlock()
					remainReqs = append(remainReqs, task)
					continue
				}
			} else {
				needCPUs = int(worker.info.Resources.CPUs) - idleCpus
			}
		}
		if int(worker.info.Resources.CPUs)-idleCpus < needCPUs+worker.cpuUsed {
			log.Infof("<%s> need %d = %d + %d CPUs but only %d available [%v / %v] [%s]",
				eschedTag, needCPUs+worker.cpuUsed, needCPUs, worker.cpuUsed,
				int(worker.info.Resources.CPUs)-idleCpus, task.sector.ID,
				taskType, worker.info.Address)
			worker.preparedTasks.mutex.Lock()
			worker.preparedTasks.queue, _ = safeRemoveWorkerRequest(worker.preparedTasks.queue, nil, task)
			worker.preparedTasks.mutex.Unlock()
			remainReqs = append(remainReqs, task)
			continue
		}
		hugepage, ok := eschedTaskHugePage[taskType]
		if ok && hugepage && 0 < worker.hugePageBytes {
			if worker.hugePageBytes < res.Memory+worker.hugePageUsed {
				log.Infof("<%s> need %d = %d + %d hugepage but only %d available [%v / %v] [%s]",
					eschedTag, res.Memory+worker.hugePageUsed, res.Memory, worker.hugePageUsed,
					worker.hugePageBytes, task.sector.ID, taskType, worker.info.Address)
				worker.preparedTasks.mutex.Lock()
				worker.preparedTasks.queue, _ = safeRemoveWorkerRequest(worker.preparedTasks.queue, nil, task)
				worker.preparedTasks.mutex.Unlock()
				remainReqs = append(remainReqs, task)
				continue
			}
		} else {
			var extraMem uint64 = 0
			if !res.DisableSwap {
				extraMem = worker.info.Resources.MemSwap
			}
			if worker.info.Resources.MemPhysical+extraMem < res.Memory+worker.memUsed {
				log.Infof("<%s> need %d = %d + %d memory but only %d available [%v / %v] [%s]",
					eschedTag, res.Memory+worker.memUsed, res.Memory, worker.memUsed,
					worker.info.Resources.MemPhysical+extraMem, task.sector.ID,
					taskType, worker.info.Address)
				worker.preparedTasks.mutex.Lock()
				worker.preparedTasks.queue, _ = safeRemoveWorkerRequest(worker.preparedTasks.queue, nil, task)
				worker.preparedTasks.mutex.Unlock()
				remainReqs = append(remainReqs, task)
				continue
			}
		}

		task.cpuUsed = needCPUs
		if 0 < len(worker.info.Resources.GPUs) {
			task.gpuUsed = res.GPUs
		}
		task.memUsed = res.Memory
		worker.acquireRequestResource(task, eschedResStageRuntime)

		worker.preparedTasks.mutex.Lock()
		worker.preparedTasks.queue, _ = safeRemoveWorkerRequest(worker.preparedTasks.queue, nil, task)
		worker.preparedTasks.mutex.Unlock()

		worker.runningTasks = append(worker.runningTasks, task)
		go bucket.runTypedTask(worker, task)
	}

	worker.preparedTasks.mutex.Lock()
	worker.preparedTasks.queue = append(remainReqs, worker.preparedTasks.queue[0:]...)
	worker.preparedTasks.mutex.Unlock()
}

func (bucket *eWorkerBucket) scheduleBucketTask() {
	log.Infof("<%s> try schedule bucket task for bucket [%d]", eschedTag, bucket.id)
	scheduled := false
	for _, worker := range bucket.workers {
		if bucket.scheduleTypedTasks(worker) == true {
			scheduled = true
		}
	}
	if scheduled {
		go func() { bucket.notifier <- struct{}{} }()
	}
}

func (bucket *eWorkerBucket) schedulePreparedTask() {
	log.Infof("<%s> try schedule prepared task for bucket [%d]", eschedTag, bucket.id)
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

func (bucket *eWorkerBucket) findBucketWorkerByID(wid uuid.UUID) *eWorkerHandle {
	for _, worker := range bucket.workers {
		if wid == worker.wid {
			return worker
		}
	}
	return nil
}

func (bucket *eWorkerBucket) findBucketWorkerIndexByID(wid uuid.UUID) int {
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
		idx := -1
		for id, task := range w.runningTasks {
			if task.id == finisher.req.id {
				idx = id
				break
			}
		}

		if 0 <= idx {
			w.runningTasks = append(w.runningTasks[:idx], w.runningTasks[idx+1:]...)
			w.releaseRequestResource(finisher.req, eschedResStageRuntime)

			if finisher.resp.err == nil {
				bucket.addCleaningTask(finisher.wid, finisher.req)
			}
		} else {
			log.Warnf("<%s> cannot find sector %v / %v in running queue [%v]",
				eschedTag, finisher.req.id, finisher.req.sector, w.info.Address)
		}
	}

	go func() { finisher.req.ret <- *finisher.resp }()
	go func() { bucket.notifier <- struct{}{} }()
	go func() { bucket.schedulerWaker <- struct{}{} }()
	go func() { bucket.schedulerRunner <- struct{}{} }()
}

func (bucket *eWorkerBucket) removeWorkerFromBucket(wid uuid.UUID) {
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
					log.Infof("<%s> return typed task %v/%v to reqQueue", task.sector.ID, task.taskType)
					tq.tasks, _ = safeRemoveWorkerRequest(tq.tasks, nil, task)
					go func(task *eWorkerRequest) {
						task.ret <- workerResponse{err: xerrors.Errorf("worker dropped unexpected")}
					}(task)
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
			log.Infof("<%s> finish task %v/%v", eschedTag, task.sector.ID, task.taskType)
			worker.preparedTasks.queue, _ = safeRemoveWorkerRequest(worker.preparedTasks.queue, nil, task)
			worker.preparedTasks.mutex.Unlock()

			go func(task *eWorkerRequest) {
				task.ret <- workerResponse{err: xerrors.Errorf("worker dropped unexpected")}
			}(task)
		}
		for {
			worker.preparingTasks.mutex.Lock()
			if 0 == len(worker.preparingTasks.queue) {
				worker.preparingTasks.mutex.Unlock()
				break
			}
			task := worker.preparingTasks.queue[0]
			log.Infof("<%s> finish preparing task %v/%v", eschedTag, task.sector.ID, task.taskType)
			worker.preparingTasks.queue, _ = safeRemoveWorkerRequest(worker.preparingTasks.queue, nil, task)
			worker.preparingTasks.mutex.Unlock()

			go func(task *eWorkerRequest) {
				task.ret <- workerResponse{err: xerrors.Errorf("worker dropped unexpected")}
			}(task)
		}
		for {
			if 0 == len(worker.runningTasks) {
				break
			}
			task := worker.runningTasks[0]
			log.Infof("<%s> finish running task %v/%v", eschedTag, task.sector.ID, task.taskType)
			worker.runningTasks, _ = safeRemoveWorkerRequest(worker.runningTasks, nil, task)

			go func(worker *eWorkerHandle, task *eWorkerRequest) {
				bucket.reqFinisher <- &eRequestFinisher{
					req:  task,
					resp: &workerResponse{err: xerrors.Errorf("worker dropped unexpected")},
					wid:  worker.wid,
				}
			}(worker, task)
		}
	}

	index := bucket.findBucketWorkerIndexByID(wid)
	if 0 <= index {
		bucket.workers = append(bucket.workers[:index], bucket.workers[index+1:]...)
		go func() { bucket.droppedWorker <- eTaskWorkerBindParam{address: worker.info.Address, wid: worker.wid} }()
		log.Infof("<%s> drop worker %v from bucket %d", eschedTag, worker.info.Address, bucket.id)
	}
	log.Infof("<%s> dropped worker %v from bucket %d", eschedTag, wid, bucket.id)
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

func (worker *eWorkerHandle) caculateTaskLimit() {
	maxReached := 0

	for key, stat := range worker.storeIDs {
		if stat.total-stat.space-stat.reserved < 100*eGiB {
			stat.maxReached = true
			worker.storeIDs[key] = stat
		}
	}

	for _, stat := range worker.storeIDs {
		if stat.maxReached {
			maxReached += 1
		}
	}

	log.Infof("<%s> storage count %v, max reached %v [%v]", eschedTag, worker.info.StorageCount, maxReached, worker.info.Address)
	if maxReached == worker.info.StorageCount {
		worker.rejectNewTask = false
	} else {
		worker.rejectNewTask = true
	}

	for _, spt := range eSealProofType {
		limit := 0

		for _, stat := range worker.storeIDs {
			limit += int(stat.space / eResourceTable[sealtasks.TTPreCommit1][spt].DiskSpace)
		}

		worker.diskConcurrentLimit[spt] = limit
		cur := worker.maxConcurrent[spt]

		if worker.supportTaskType(sealtasks.TTPreCommit1) {
			cur[sealtasks.TTPreCommit1] = worker.diskConcurrentLimit[spt]
			cur[sealtasks.TTAddPiece] = worker.diskConcurrentLimit[spt]
		}
		if worker.maxRuntimeConcurrent[spt]*2 < cur[sealtasks.TTPreCommit1] {
			worker.maxRuntimeConcurrent[spt] *= 2
		} else {
			worker.maxRuntimeConcurrent[spt] = cur[sealtasks.TTPreCommit1]
		}
		if worker.supportTaskType(sealtasks.TTPreCommit2) {
			cur[sealtasks.TTPreCommit2] = worker.diskConcurrentLimit[spt]
		}

		log.Debugf("<%s> update max concurrent for (%v) %v = %v [%s]",
			eschedTag, spt,
			sealtasks.TTPreCommit1,
			cur[sealtasks.TTPreCommit1],
			worker.info.Address)

		worker.maxConcurrent[spt] = cur
	}
}

func (bucket *eWorkerBucket) onAddStore(w *eWorkerHandle, act eStoreAction) {
	if _, ok := w.storeIDs[act.id]; ok {
		stat := w.storeIDs[act.id]
		if stat.space < act.stat.space {
			log.Infof("<%s> store %v is updating %v -> %v (%v | %v)",
				eschedTag, act.id, stat.space, act.stat.space, stat.maxReached, act.stat.total)
			act.stat.maxReached = stat.maxReached
			w.storeIDs[act.id] = act.stat
		}
	} else {
		log.Infof("<%s> store %v is adding %v | %v", eschedTag, act.id, act.stat.space, act.stat.total)
		w.storeIDs[act.id] = act.stat
	}

	w.caculateTaskLimit()
	w.state = eschedWorkerStateReady
}

func (bucket *eWorkerBucket) onDropStore(w *eWorkerHandle, act eStoreAction) {
	if _, ok := w.storeIDs[act.id]; !ok {
		log.Infof("<%s> store %v already dropped", eschedTag, act.id)
		return
	}
	log.Infof("<%s> store %v already dropped", eschedTag, act.id)
	delete(w.storeIDs, act.id)
	w.caculateTaskLimit()
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

func (bucket *eWorkerBucket) onTaskClean(clean *eWorkerTaskCleaning) {
	for _, worker := range bucket.workers {
		if !worker.info.BigCache && clean.byCacheMove {
			log.Debugf("<%s> task %v / %v cannot removed by move cache done from worker %s",
				eschedTag, clean.sector, clean.taskType, worker.info.Address)
			continue
		}
		for idx, task := range worker.cleaningTasks {
			if (clean.taskType == task.taskType || clean.bySectorRemove) && task.sector.ID.Number == clean.sector.ID.Number {
				log.Infof("<%s> clean task %v / %v [byCacheMove %v] from %s [bigCache %v]",
					eschedTag, clean.sector, clean.taskType, clean.byCacheMove,
					worker.info.Address, worker.info.BigCache)
				worker.cleaningTasks = append(worker.cleaningTasks[:idx], worker.cleaningTasks[idx+1:]...)
				go func() { bucket.notifier <- struct{}{} }()
				go func() { bucket.schedulerWaker <- struct{}{} }()
				go func() { bucket.schedulerRunner <- struct{}{} }()
				if !clean.bySectorRemove {
					return
				}
			}
		}
	}
}

func (bucket *eWorkerBucket) onWorkerStatsQuery(param *eWorkerStatsParam) {
	out := map[uuid.UUID]storiface.WorkerStats{}

	for _, worker := range bucket.workers {
		workerStores := []storiface.WorkerStore{}
		for _, stat := range worker.storeIDs {
			workerStores = append(workerStores, storiface.WorkerStore{
				Total:      stat.total,
				Available:  stat.space,
				Reserved:   stat.reserved,
				MaxReached: stat.maxReached,
				ID:         string(stat.id),
			})
		}
		out[worker.wid] = storiface.WorkerStats{
			Info:          worker.info,
			GpuUsed:       0 < worker.gpuUsed,
			CpuUse:        uint64(worker.cpuUsed),
			Tasks:         make(map[sealtasks.TaskType]storiface.TasksInfo),
			State:         worker.state,
			Maintaining:   worker.maintaining,
			RejectNewTask: worker.rejectNewTask,
			Stores:        workerStores,
		}
		for _, taskType := range worker.info.SupportTasks {
			out[worker.wid].Tasks[taskType] = storiface.TasksInfo{
				Waiting:       0,
				Running:       0,
				Prepared:      0,
				MaxConcurrent: worker.maxConcurrent[bucket.spt][taskType],
			}
		}
		for _, pq := range worker.priorityTasksQueue {
			for taskType, tq := range pq.typedTasksQueue {
				info := out[worker.wid].Tasks[taskType]
				info.Waiting += len(tq.tasks)
				out[worker.wid].Tasks[taskType] = info
			}
		}
		worker.preparingTasks.mutex.Lock()
		for _, task := range worker.preparingTasks.queue {
			info := out[worker.wid].Tasks[task.taskType]
			info.Prepared += 1
			out[worker.wid].Tasks[task.taskType] = info
		}
		worker.preparingTasks.mutex.Unlock()
		worker.preparedTasks.mutex.Lock()
		for _, task := range worker.preparedTasks.queue {
			info := out[worker.wid].Tasks[task.taskType]
			info.Prepared += 1
			out[worker.wid].Tasks[task.taskType] = info
		}
		worker.preparedTasks.mutex.Unlock()
		for _, task := range worker.runningTasks {
			info := out[worker.wid].Tasks[task.taskType]
			info.Running += 1
			out[worker.wid].Tasks[task.taskType] = info
		}
		for _, task := range worker.cleaningTasks {
			info := out[worker.wid].Tasks[task.taskType]
			info.Cleaning += 1
			out[worker.wid].Tasks[task.taskType] = info
		}
	}

	go func() { param.resp <- out }()
}

func (bucket *eWorkerBucket) onWorkerJobsQuery(param *eWorkerJobsParam) {
	out := map[uuid.UUID][]storiface.WorkerJob{}

	for _, worker := range bucket.workers {
		for _, task := range worker.runningTasks {
			out[worker.wid] = append(out[worker.wid], storiface.WorkerJob{
				ID:       storiface.CallID{Sector: task.sector.ID, ID: task.uuid},
				Sector:   task.sector.ID,
				Task:     task.taskType,
				Start:    task.startRunTimeRaw,
				Hostname: worker.info.Address,
			})
		}

		wi := 0
		worker.preparedTasks.mutex.Lock()
		for _, task := range worker.preparedTasks.queue {
			out[worker.wid] = append(out[worker.wid], storiface.WorkerJob{
				ID:       storiface.CallID{Sector: task.sector.ID, ID: task.uuid},
				Sector:   task.sector.ID,
				Task:     task.taskType,
				RunWait:  wi + 1,
				Start:    task.inqueueTimeRaw,
				Hostname: worker.info.Address,
			})
			wi += 1
		}
		worker.preparedTasks.mutex.Unlock()

		worker.preparingTasks.mutex.Lock()
		for _, task := range worker.preparingTasks.queue {
			out[worker.wid] = append(out[worker.wid], storiface.WorkerJob{
				ID:       storiface.CallID{Sector: task.sector.ID, ID: task.uuid},
				Sector:   task.sector.ID,
				Task:     task.taskType,
				RunWait:  wi + 1 + 1000,
				Start:    task.inqueueTimeRaw,
				Hostname: worker.info.Address,
			})
			wi += 1
		}
		worker.preparingTasks.mutex.Unlock()

		wi = 0
		for _, pq := range worker.priorityTasksQueue {
			for _, tq := range pq.typedTasksQueue {
				for _, task := range tq.tasks {
					out[worker.wid] = append(out[worker.wid], storiface.WorkerJob{
						ID:       storiface.CallID{Sector: task.sector.ID, ID: task.uuid},
						Sector:   task.sector.ID,
						Task:     task.taskType,
						RunWait:  100000*(pq.priority+1) + 1 + wi,
						Start:    task.inqueueTimeRaw,
						Hostname: worker.info.Address,
					})
					wi += 1
				}
			}
		}
	}

	go func() { param.resp <- out }()
}

func (bucket *eWorkerBucket) waitingJobs(worker *eWorkerHandle, taskType sealtasks.TaskType) int {
	waitingJobs := 0
	bucket.reqQueue.mutex.Lock()
	if reqs, ok := bucket.reqQueue.reqs[taskType]; ok {
		for _, req := range reqs {
			bucket.taskWorkerBinder.mutex.Lock()
			if binder, ok := bucket.taskWorkerBinder.binder[req.sector.ID.Number]; ok {
				if binder.address == worker.info.Address && binder.wid == worker.wid {
					waitingJobs += 1
				}
			}
			bucket.taskWorkerBinder.mutex.Unlock()
		}
	}
	bucket.reqQueue.mutex.Unlock()
	return waitingJobs
}

func (bucket *eWorkerBucket) onBucketPledgedJobs(param *eBucketPledgedJobsParam) {
	var jobs int = 0
	for _, worker := range bucket.workers {
		if eschedWorkerStateWaving == worker.state || worker.rejectNewTask || worker.maintaining {
			continue
		}

		waitingJobs := bucket.waitingJobs(worker, sealtasks.TTPreCommit1)
		bucket.reqQueue.mutex.Lock()
		waitingJobs += len(bucket.reqQueue.reqs[sealtasks.TTAddPiece])
		bucket.reqQueue.mutex.Unlock()

		if 0 < waitingJobs {
			continue
		}

		taskCount := worker.typedTaskCount(sealtasks.TTPreCommit1, true)
		if taskTypes, ok := eschedTaskLimitMerge[sealtasks.TTPreCommit1]; ok {
			for _, lTaskType := range taskTypes {
				taskCount += worker.typedTaskCount(lTaskType, false)
			}
		}
		taskCount += worker.typedTaskCount(sealtasks.TTAddPiece, true)

		bucket.reqQueue.mutex.Lock()
		if reqs, ok := bucket.reqQueue.reqs[sealtasks.TTPreCommit1]; ok {
			for _, req := range reqs {
				bucket.taskWorkerBinder.mutex.Lock()
				if binder, ok := bucket.taskWorkerBinder.binder[req.sector.ID.Number]; ok {
					if binder.address == worker.info.Address && binder.wid == worker.wid {
						taskCount += 1
					}
				}
				bucket.taskWorkerBinder.mutex.Unlock()
			}
		}

		if taskTypes, ok := eschedTaskLimitMerge[sealtasks.TTPreCommit1]; ok {
			for _, taskType := range taskTypes {
				if reqs, ok := bucket.reqQueue.reqs[taskType]; ok {
					for _, req := range reqs {
						bucket.taskWorkerBinder.mutex.Lock()
						if binder, ok := bucket.taskWorkerBinder.binder[req.sector.ID.Number]; ok {
							if binder.address == worker.info.Address && binder.wid == worker.wid {
								taskCount += 1
							}
						}
						bucket.taskWorkerBinder.mutex.Unlock()
					}
				}
			}
		}
		bucket.reqQueue.mutex.Unlock()

		if taskCount == 0 {
			jobs += worker.maxRuntimeConcurrent[bucket.spt]
		}

		if 0 < worker.maxRuntimeConcurrent[bucket.spt]-taskCount {
			jobs += (worker.maxRuntimeConcurrent[bucket.spt] - taskCount)
		}
	}
	go func(jobs int) { param.resp <- jobs }(jobs)
}

func (bucket *eWorkerBucket) onScheduleTick() {
	go func() { bucket.notifier <- struct{}{} }()
	go func() { bucket.schedulerWaker <- struct{}{} }()
	go func() { bucket.schedulerRunner <- struct{}{} }()
}

func (bucket *eWorkerBucket) setTaskUUID(uuid eTaskUUID) {
	log.Infof("<%s> try to set sector %v uuid to %v", eschedTag, uuid.sector.ID, uuid.uuid)
	for _, worker := range bucket.workers {
		for _, task := range worker.runningTasks {
			if task.sector.ID == uuid.sector.ID {
				task.uuid = uuid.uuid
				return
			}
		}

		worker.preparedTasks.mutex.Lock()
		for _, task := range worker.preparedTasks.queue {
			if task.sector.ID == uuid.sector.ID {
				task.uuid = uuid.uuid
				worker.preparedTasks.mutex.Unlock()
				return
			}
		}
		worker.preparedTasks.mutex.Unlock()

		worker.preparingTasks.mutex.Lock()
		for _, task := range worker.preparingTasks.queue {
			if task.sector.ID == uuid.sector.ID {
				task.uuid = uuid.uuid
				worker.preparingTasks.mutex.Unlock()
				return
			}
		}
		worker.preparingTasks.mutex.Unlock()

		for _, pq := range worker.priorityTasksQueue {
			for _, tq := range pq.typedTasksQueue {
				for _, task := range tq.tasks {
					if task.sector.ID == uuid.sector.ID {
						task.uuid = uuid.uuid
						return
					}
				}
			}
		}
	}
}

func (bucket *eWorkerBucket) onAbortTask(sector storage.SectorRef) {
	for _, worker := range bucket.workers {
		for _, task := range worker.runningTasks {
			if task.sector.ID == sector.ID {
				go func(worker *eWorkerHandle, task *eWorkerRequest) {
					bucket.reqFinisher <- &eRequestFinisher{
						req:  task,
						resp: &workerResponse{err: xerrors.Errorf("aborted by user")},
						wid:  worker.wid,
					}
				}(worker, task)
				continue
			}
		}

		remainReqs := make([]*eWorkerRequest, 0)
		worker.preparedTasks.mutex.Lock()
		for _, task := range worker.preparedTasks.queue {
			if task.sector.ID == sector.ID {
				go func(task *eWorkerRequest) {
					task.ret <- workerResponse{err: xerrors.Errorf("aborted by user")}
				}(task)
				continue
			}
			remainReqs = append(remainReqs, task)
		}
		worker.preparedTasks.queue = remainReqs
		worker.preparedTasks.mutex.Unlock()

		remainReqs = make([]*eWorkerRequest, 0)
		worker.preparingTasks.mutex.Lock()
		for _, task := range worker.preparingTasks.queue {
			if task.sector.ID == sector.ID {
				go func(task *eWorkerRequest) {
					task.ret <- workerResponse{err: xerrors.Errorf("aborted by user")}
				}(task)
				continue
			}
			remainReqs = append(remainReqs, task)
		}
		worker.preparingTasks.queue = remainReqs
		worker.preparingTasks.mutex.Unlock()

		for _, pq := range worker.priorityTasksQueue {
			for _, tq := range pq.typedTasksQueue {
				remainReqs = make([]*eWorkerRequest, 0)
				for _, task := range tq.tasks {
					if task.sector.ID == sector.ID {
						go func(task *eWorkerRequest) {
							task.ret <- workerResponse{err: xerrors.Errorf("aborted by user")}
						}(task)
						continue
					}
					remainReqs = append(remainReqs, task)
				}
				tq.tasks = remainReqs
			}
		}
	}
}

func (bucket *eWorkerBucket) onSetWorkerMode(workerMode eWorkerMode) {
	for _, worker := range bucket.workers {
		if workerMode.address == worker.info.Address {
			if "maintaining" == workerMode.mode {
				worker.maintaining = true
			} else {
				worker.maintaining = false
			}
		}
	}
}

func (bucket *eWorkerBucket) onSetWorkerReservedSpace(workerReservedSpace eWorkerReservedSpace) {
	for _, worker := range bucket.workers {
		if workerReservedSpace.address == worker.info.Address {
			for key, stat := range worker.storeIDs {
				if strings.HasPrefix(string(stat.id), workerReservedSpace.storePrefix) {
					stat.reserved = workerReservedSpace.reserved
					worker.storeIDs[key] = stat
					return
				}
			}
		}
	}
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
		case uuid := <-bucket.taskUUID:
			bucket.setTaskUUID(uuid)
		case sector := <-bucket.abortTask:
			bucket.onAbortTask(sector)
		case workerMode := <-bucket.setWorkerMode:
			bucket.onSetWorkerMode(workerMode)
		case workerReservedSpace := <-bucket.setWorkerReservedSpace:
			bucket.onSetWorkerReservedSpace(workerReservedSpace)
		case wid := <-bucket.dropWorker:
			bucket.removeWorkerFromBucket(wid)
		case act := <-bucket.storageNotifier:
			bucket.onStorageNotify(act)
		case clean := <-bucket.taskCleanerHandler:
			bucket.onTaskClean(clean)
		case param := <-bucket.workerStatsQuery:
			bucket.onWorkerStatsQuery(param)
		case param := <-bucket.workerJobsQuery:
			bucket.onWorkerJobsQuery(param)
		case param := <-bucket.bucketPledgedJobs:
			bucket.onBucketPledgedJobs(param)
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
	go func() { bucket.newWorker <- w }()
}

func newExtScheduler() *edispatcher {
	dispatcher := &edispatcher{
		nextWorker:             0,
		newWorker:              make(chan *eWorkerHandle, 40),
		dropWorker:             make(chan uuid.UUID, 10),
		taskUUID:               make(chan eTaskUUID, 10),
		setWorkerMode:          make(chan eWorkerMode, 10),
		setWorkerReservedSpace: make(chan eWorkerReservedSpace, 10),
		abortTask:              make(chan storage.SectorRef, 10),
		newRequest:             make(chan *eWorkerRequest, 1000),
		buckets:                make([]*eWorkerBucket, eschedWorkerBuckets),
		reqQueue: &eRequestQueue{
			reqs: make(map[sealtasks.TaskType][]*eWorkerRequest),
		},
		storageNotifier: make(chan eStoreAction, 10),
		droppedWorker:   make(chan eTaskWorkerBindParam, 10),
		taskCleaner:     make(chan *eWorkerTaskCleaning, 10),
		closing:         make(chan struct{}, 2),
		taskWorkerBinder: &eTaskWorkerBinder{
			binder: make(map[abi.SectorNumber]eTaskWorkerBindParam),
		},
		workerStatsQuery:  make(chan *eWorkerStatsParam, 10),
		workerJobsQuery:   make(chan *eWorkerJobsParam, 10),
		bucketPledgedJobs: make(chan *eBucketPledgedJobsParam, 0),
		workerJobs:        make(map[uuid.UUID][]storiface.WorkerJob),
		workerStats:       make(map[uuid.UUID]storiface.WorkerStats),
		workerJobsResp:    make(chan map[uuid.UUID][]storiface.WorkerJob, 10),
		workerStatsResp:   make(chan map[uuid.UUID]storiface.WorkerStats, 10),
		statsTicker:       time.NewTicker(2 * time.Minute),
	}

	for i := range dispatcher.buckets {
		dispatcher.buckets[i] = &eWorkerBucket{
			spt:                    abi.RegisteredSealProof_StackedDrg32GiBV1,
			id:                     i,
			newWorker:              make(chan *eWorkerHandle),
			workers:                make([]*eWorkerHandle, 0),
			reqQueue:               dispatcher.reqQueue,
			concurrentAP:           1,
			idleCpus:               4,
			schedulerWaker:         make(chan struct{}, 20),
			schedulerRunner:        make(chan struct{}, 20000),
			reqFinisher:            make(chan *eRequestFinisher),
			notifier:               make(chan struct{}),
			dropWorker:             make(chan uuid.UUID, 10),
			taskUUID:               make(chan eTaskUUID, 10),
			setWorkerMode:          make(chan eWorkerMode, 10),
			setWorkerReservedSpace: make(chan eWorkerReservedSpace, 10),
			abortTask:              make(chan storage.SectorRef, 10),
			retRequest:             dispatcher.newRequest,
			storageNotifier:        make(chan eStoreAction, 10),
			droppedWorker:          dispatcher.droppedWorker,
			taskWorkerBinder:       dispatcher.taskWorkerBinder,
			taskCleaner:            dispatcher.taskCleaner,
			taskCleanerHandler:     make(chan *eWorkerTaskCleaning, 10),
			workerStatsQuery:       make(chan *eWorkerStatsParam, 10),
			workerJobsQuery:        make(chan *eWorkerJobsParam, 10),
			closing:                make(chan struct{}, 2),
			ticker:                 time.NewTicker(3 * 60 * time.Second),
			bucketPledgedJobs:      make(chan *eBucketPledgedJobsParam, 0),
		}
		go dispatcher.buckets[i].scheduler()
	}

	debugEnable := os.Getenv("MINER_ESCHED_DEBUG")
	if "true" == debugEnable {
		log.Infof("<%s> enable debug for miner esched", eschedTag)
		eschedDebug = true
	}

	eResourceTable[sealtasks.TTUnseal] = eResourceTable[sealtasks.TTPreCommit1]
	eResourceTable[sealtasks.TTFinalize] = eResourceTable[sealtasks.TTCommit1]
	eResourceTable[sealtasks.TTFetch] = eResourceTable[sealtasks.TTCommit1]
	eResourceTable[sealtasks.TTReadUnsealed] = eResourceTable[sealtasks.TTCommit1]

	for _, m := range eResourceTable {
		m[abi.RegisteredSealProof_StackedDrg2KiBV1_1] = m[abi.RegisteredSealProof_StackedDrg2KiBV1]
		m[abi.RegisteredSealProof_StackedDrg8MiBV1_1] = m[abi.RegisteredSealProof_StackedDrg8MiBV1]
		m[abi.RegisteredSealProof_StackedDrg512MiBV1_1] = m[abi.RegisteredSealProof_StackedDrg512MiBV1]
		m[abi.RegisteredSealProof_StackedDrg32GiBV1_1] = m[abi.RegisteredSealProof_StackedDrg32GiBV1]
		m[abi.RegisteredSealProof_StackedDrg64GiBV1_1] = m[abi.RegisteredSealProof_StackedDrg64GiBV1]
	}

	return dispatcher
}

func (sh *edispatcher) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	if sealtasks.TTPreCommit1 == taskType {
		sh.taskWorkerBinder.mutex.Lock()
		if _, ok := sh.taskWorkerBinder.binder[sector.ID.Number]; !ok {
			sh.taskWorkerBinder.mutex.Unlock()
			return xerrors.Errorf("not binded to any worker")
		}
		sh.taskWorkerBinder.mutex.Unlock()
	}

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
		priority: getTaskPriority(taskType),
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
	priority, _ := eTaskPriority[taskType]
	return priority
}

func (w *eWorkerHandle) supportTaskType(taskType sealtasks.TaskType) bool {
	for _, lTaskType := range w.info.SupportTasks {
		if taskType == lTaskType {
			return true
		}
	}
	return false
}

func (sh *edispatcher) addNewWorkerToBucket(w *eWorkerHandle) {
	w.patchLocalhost()

	log.Infof("<%s> add new worker %s to bucket", eschedTag, w.info.Address)
	w.dumpWorkerInfo()

	w.priorityTasksQueue = make([]*eWorkerReqPriorityList, len(eTaskPriority))
	for i := 0; i < len(w.priorityTasksQueue); i++ {
		w.priorityTasksQueue[i] = &eWorkerReqPriorityList{
			typedTasksQueue: make(map[sealtasks.TaskType]*eWorkerReqTypedList),
			priority:        i + 1,
		}
	}

	for _, taskType := range w.info.SupportTasks {
		priority := getTaskPriority(taskType)
		w.priorityTasksQueue[priority-1].typedTasksQueue[taskType] = &eWorkerReqTypedList{
			taskType: taskType,
			tasks:    make([]*eWorkerRequest, 0),
		}
	}

	w.maintaining = false
	w.rejectNewTask = true

	w.storeIDs = make(map[stores.ID]eStoreStat)
	w.runningTasks = make([]*eWorkerRequest, 0)
	w.preparedTasks = &eWorkerSyncTaskList{
		queue: make([]*eWorkerRequest, 0),
	}
	w.preparingTasks = &eWorkerSyncTaskList{
		queue: make([]*eWorkerRequest, 0),
	}
	w.cleaningTasks = make([]*eWorkerTaskCleaning, 0)

	w.diskConcurrentLimit = make(map[abi.RegisteredSealProof]int)
	w.memoryConcurrentLimit = make(map[abi.RegisteredSealProof]int)
	w.maxRuntimeConcurrent = make(map[abi.RegisteredSealProof]int)
	w.maxConcurrent = make(map[abi.RegisteredSealProof]map[sealtasks.TaskType]int)

	w.hugePageBytes = uint64(w.info.Resources.HugePages) * w.info.Resources.HugePageSize
	w.memUsed = w.hugePageBytes

	for _, spt := range eSealProofType {
		cur := make(map[sealtasks.TaskType]int)

		if w.supportTaskType(sealtasks.TTPreCommit1) {
			pc1Memory := w.info.Resources.MemPhysical
			if 0 < w.hugePageBytes {
				pc1Memory = w.hugePageBytes
			} else {
				if w.supportTaskType(sealtasks.TTPreCommit2) {
					pc1Memory -= eResourceTable[sealtasks.TTPreCommit2][spt].Memory
				}
				if w.supportTaskType(sealtasks.TTCommit2) {
					pc1Memory -= eResourceTable[sealtasks.TTCommit1][spt].Memory
				}
			}

			var limit1 int = int(pc1Memory / eResourceTable[sealtasks.TTPreCommit1][spt].Memory)
			var limit2 int = int(w.info.Resources.CPUs * 90 / 100)

			w.memoryConcurrentLimit[spt] = limit1
			cur[sealtasks.TTPreCommit1] = limit1
			if limit2 < cur[sealtasks.TTPreCommit1] {
				cur[sealtasks.TTPreCommit1] = limit2
			}
			w.maxRuntimeConcurrent[spt] = cur[sealtasks.TTPreCommit1]

			cur[sealtasks.TTAddPiece] = cur[sealtasks.TTPreCommit1]
			cur[sealtasks.TTUnseal] = cur[sealtasks.TTPreCommit1]

			log.Infof("<%s> max concurrent for (%v) %v = %v [%s]",
				eschedTag, spt,
				sealtasks.TTPreCommit1,
				cur[sealtasks.TTPreCommit1],
				w.info.Address)
		}

		if w.supportTaskType(sealtasks.TTPreCommit2) {
			if w.supportTaskType(sealtasks.TTPreCommit1) {
				cur[sealtasks.TTPreCommit2] = cur[sealtasks.TTPreCommit1]
			} else {
				cur[sealtasks.TTPreCommit2] = 1280
			}
			log.Infof("<%s> max concurrent for %v = %v [%s]",
				eschedTag,
				sealtasks.TTPreCommit2,
				cur[sealtasks.TTPreCommit2],
				w.info.Address)
		}

		if w.supportTaskType(sealtasks.TTCommit1) {
			cur[sealtasks.TTCommit1] = 1280
		}
		if w.supportTaskType(sealtasks.TTFinalize) {
			cur[sealtasks.TTFinalize] = 1280
		}
		if w.supportTaskType(sealtasks.TTFetch) {
			cur[sealtasks.TTFetch] = 1280
		}
		if w.supportTaskType(sealtasks.TTReadUnsealed) {
			cur[sealtasks.TTReadUnsealed] = 1280
		}

		if w.supportTaskType(sealtasks.TTCommit2) {
			cur[sealtasks.TTCommit2] = len(w.info.Resources.GPUs)

			c2Memory := w.info.Resources.MemPhysical + w.info.Resources.MemSwap
			c2Memory -= w.memUsed

			var limit int = int(c2Memory / eResourceTable[sealtasks.TTCommit2][spt].Memory)
			if limit < cur[sealtasks.TTCommit2] || 0 == cur[sealtasks.TTCommit2] {
				cur[sealtasks.TTCommit2] = limit
			}
			log.Infof("<%s> max concurrent for %v = %v [%s]",
				eschedTag,
				sealtasks.TTCommit2,
				cur[sealtasks.TTCommit2],
				w.info.Address)
		}

		w.maxConcurrent[spt] = cur
	}

	w.state = eschedWorkerStateWaving

	workerBucketIndex := w.wIndex % eschedWorkerBuckets
	bucket := sh.buckets[workerBucketIndex]

	bucket.addNewWorker(w)
	log.Infof("<%s> added new worker %s to bucket", eschedTag, w.info.Address)
}

func (req *eWorkerRequest) dumpWorkerRequest() {
	log.Debugf("Task Information -------")
	log.Debugf("  Sector ID: --------------- %v", req.sector.ID)
	log.Debugf("  Task Type: --------------- %v", req.taskType)
}

func (sh *edispatcher) addNewWorkerRequestToBucketWorker(req *eWorkerRequest) {
	log.Infof("<%s> add new request %v/%v to request queue", eschedTag, req.sector.ID, req.taskType)
	req.dumpWorkerRequest()

	req.id = sh.nextRequest
	sh.nextRequest += 1
	req.requestTime = time.Now().UnixNano()
	req.deadTime = req.requestTime + eTaskTimeout[req.taskType]
	req.requestTimeRaw = time.Now()

	sh.reqQueue.mutex.Lock()
	if _, ok := sh.reqQueue.reqs[req.taskType]; !ok {
		sh.reqQueue.reqs[req.taskType] = make([]*eWorkerRequest, 0)
	}
	sh.reqQueue.reqs[req.taskType] = append(sh.reqQueue.reqs[req.taskType], req)
	sh.reqQueue.mutex.Unlock()

	start := rand.Intn(len(sh.buckets))
	for i := 0; i < len(sh.buckets); i++ {
		bucket := sh.buckets[(i+start)%len(sh.buckets)]
		go func(bucket *eWorkerBucket) { bucket.notifier <- struct{}{} }(bucket)
	}
	log.Infof("<%s> added new request %v/%v to request queue", eschedTag, req.sector, req.taskType)
}

func (sh *edispatcher) dropWorkerFromBucket(wid uuid.UUID) {
	log.Infof("<%s> drop worker %v from all bucket", eschedTag, wid)
	for _, bucket := range sh.buckets {
		go func(bucket *eWorkerBucket) { bucket.dropWorker <- wid }(bucket)
	}
	log.Infof("<%s> dropped worker %v from all bucket", eschedTag, wid)
}

func (sh *edispatcher) onStorageNotify(act eStoreAction) {
	log.Infof("<%s> %v store %v", eschedTag, act.act, act.id)
	for _, bucket := range sh.buckets {
		go func(bucket *eWorkerBucket) { bucket.storageNotifier <- act }(bucket)
	}
}

func (sh *edispatcher) onWorkerDropped(param eTaskWorkerBindParam) {
	sh.taskWorkerBinder.mutex.Lock()
	for sector, binder := range sh.taskWorkerBinder.binder {
		if binder.address == param.address && binder.wid == param.wid {
			delete(sh.taskWorkerBinder.binder, sector)
		}
	}
	sh.taskWorkerBinder.mutex.Unlock()
}

func (sh *edispatcher) closeAllBuckets() {
	for _, bucket := range sh.buckets {
		go func(bucket *eWorkerBucket) { bucket.closing <- struct{}{} }(bucket)
	}
}

func (sh *edispatcher) watchWorkerClosing(w *eWorkerHandle) {
	for {
		ctx, cancel := context.WithTimeout(context.TODO(), 180*time.Second)
		session, err := w.w.Session(ctx)
		cancel()
		if nil != err || ClosedWorkerID == session || w.wid != session {
			log.Warnf("drop worker %v by [%v]", w.wid, err)
			sh.dropWorker <- w.wid
			return
		}
		time.Sleep(3 * time.Second)
	}
}

func (sh *edispatcher) addWorkerClosingWatcher(w *eWorkerHandle) {
	// It's ugly but I think it's not a problem because most of the time the watcher is sleep
	go sh.watchWorkerClosing(w)
}

func (sh *edispatcher) onTaskClean(clean *eWorkerTaskCleaning) {
	for _, bucket := range sh.buckets {
		go func(bucket *eWorkerBucket) { bucket.taskCleanerHandler <- clean }(bucket)
	}
}

func (sh *edispatcher) onWorkerJobsQuery(param *eWorkerJobsParam) {
	go func(param *eWorkerJobsParam) {
		out := map[uuid.UUID][]storiface.WorkerJob{}
		resps := make([]chan map[uuid.UUID][]storiface.WorkerJob, len(sh.buckets))
		for i, _ := range sh.buckets {
			resps[i] = make(chan map[uuid.UUID][]storiface.WorkerJob)
		}
		for i, bucket := range sh.buckets {
			resp := resps[i]
			queryParam := &eWorkerJobsParam{
				command: param.command,
				resp:    resp,
			}
			go func(bucket *eWorkerBucket, queryParam *eWorkerJobsParam) {
				bucket.workerJobsQuery <- queryParam
			}(bucket, queryParam)
		}
		for i, _ := range sh.buckets {
			resp := resps[i]
			select {
			case r := <-resp:
				for k, v := range r {
					out[uuid.UUID(k)] = v
				}
			}
		}
		sh.reqQueue.mutex.Lock()
		wi := 5000000
		for taskType, reqs := range sh.reqQueue.reqs {
			if _, ok := out[eschedUnassignedWorker]; !ok {
				out[eschedUnassignedWorker] = make([]storiface.WorkerJob, 0)
			}
			for _, req := range reqs {
				address := "unbinded"
				sh.taskWorkerBinder.mutex.Lock()
				if binder, ok := sh.taskWorkerBinder.binder[req.sector.ID.Number]; ok {
					address = binder.address
				}
				sh.taskWorkerBinder.mutex.Unlock()
				out[eschedUnassignedWorker] = append(out[eschedUnassignedWorker], storiface.WorkerJob{
					ID:       storiface.CallID{Sector: req.sector.ID, ID: req.uuid},
					Sector:   req.sector.ID,
					Task:     taskType,
					RunWait:  wi + 1,
					Start:    req.requestTimeRaw,
					Hostname: address,
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
		out := map[uuid.UUID]storiface.WorkerStats{}
		resps := make([]chan map[uuid.UUID]storiface.WorkerStats, len(sh.buckets))
		for i, _ := range sh.buckets {
			resps[i] = make(chan map[uuid.UUID]storiface.WorkerStats)
		}
		for i, bucket := range sh.buckets {
			resp := resps[i]
			queryParam := &eWorkerStatsParam{
				command: param.command,
				resp:    resp,
			}
			go func(bucket *eWorkerBucket, queryParam *eWorkerStatsParam) {
				bucket.workerStatsQuery <- queryParam
			}(bucket, queryParam)
		}
		for i, _ := range sh.buckets {
			resp := resps[i]
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

func (sh *edispatcher) onStatsTick() {
	statsParam := &eWorkerStatsParam{
		command: eschedWorkerStats,
		resp:    sh.workerStatsResp,
	}
	sh.onWorkerStatsQuery(statsParam)

	jobsParam := &eWorkerJobsParam{
		command: eschedWorkerJobs,
		resp:    sh.workerJobsResp,
	}
	sh.onWorkerJobsQuery(jobsParam)
}

func (sh *edispatcher) onWorkerJobsResp(jobs map[uuid.UUID][]storiface.WorkerJob) {
	sh.workerJobs = jobs
}

func (sh *edispatcher) onWorkerStatsResp(stats map[uuid.UUID]storiface.WorkerStats) {
	sh.workerStats = stats
}

func (sh *edispatcher) onCmdWorkerJobsQuery(param *eWorkerJobsParam) {
	go func() { param.resp <- sh.workerJobs }()
}

func (sh *edispatcher) onCmdWorkerStatsQuery(param *eWorkerStatsParam) {
	go func() { param.resp <- sh.workerStats }()
}

func (sh *edispatcher) onBucketPledgedJobs(param *eBucketPledgedJobsParam) {
	go func(param *eBucketPledgedJobsParam) {
		var jobs int = 0
		resps := make([]chan int, len(sh.buckets))
		for i, _ := range sh.buckets {
			resps[i] = make(chan int)
		}
		for i, bucket := range sh.buckets {
			resp := resps[i]
			jobsParam := &eBucketPledgedJobsParam{
				resp: resp,
			}
			go func(bucket *eWorkerBucket, jobsParam *eBucketPledgedJobsParam) {
				bucket.bucketPledgedJobs <- jobsParam
			}(bucket, jobsParam)
		}
		for i, _ := range sh.buckets {
			resp := resps[i]
			select {
			case count := <-resp:
				jobs += count
			}
		}
		param.resp <- jobs
	}(param)
}

func (sh *edispatcher) onTaskUUID(uuid eTaskUUID) {
	for _, bucket := range sh.buckets {
		go func(bucket *eWorkerBucket) { bucket.taskUUID <- uuid }(bucket)
	}
}

func (sh *edispatcher) onAbortTask(sector storage.SectorRef) {
	sh.reqQueue.mutex.Lock()
	for taskType, reqs := range sh.reqQueue.reqs {
		remainReqs := make([]*eWorkerRequest, 0)
		for _, req := range reqs {
			if req.sector.ID == sector.ID {
				go func(req *eWorkerRequest) {
					req.ret <- workerResponse{err: xerrors.Errorf("aborted by user")}
				}(req)
				continue
			}
			remainReqs = append(remainReqs, req)
		}
		sh.reqQueue.reqs[taskType] = remainReqs
	}
	sh.reqQueue.mutex.Unlock()

	for _, bucket := range sh.buckets {
		go func(bucket *eWorkerBucket, sector storage.SectorRef) { bucket.abortTask <- sector }(bucket, sector)
	}
}

func (sh *edispatcher) onSetWorkerMode(workerMode eWorkerMode) {
	for _, bucket := range sh.buckets {
		go func(bucket *eWorkerBucket) { bucket.setWorkerMode <- workerMode }(bucket)
	}
}

func (sh *edispatcher) onSetWorkerReservedSpace(workerReservedSpace eWorkerReservedSpace) {
	for _, bucket := range sh.buckets {
		go func(bucket *eWorkerBucket) { bucket.setWorkerReservedSpace <- workerReservedSpace }(bucket)
	}
}

func (sh *edispatcher) statePrinter() {
	ticker := time.NewTicker(3 * time.Minute)
	for {
		select {
		case <-ticker.C:
			log.Infof("<%s> dispatcher state %s", eschedTag, sh.state)
		}
	}
}

func (sh *edispatcher) runSched() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	go sh.statePrinter()
	sh.ctx = ctx

	for {
		select {
		case req := <-sh.newRequest:
			sh.state = "newRequestStart"
			sh.addNewWorkerRequestToBucketWorker(req)
			sh.state = "newRequestEnd"
		case w := <-sh.newWorker:
			sh.state = "newWorkerStart"
			sh.addWorkerClosingWatcher(w)
			sh.addNewWorkerToBucket(w)
			sh.state = "newWorkerEnd"
		case wid := <-sh.dropWorker:
			sh.state = "dropWorkerStart"
			sh.dropWorkerFromBucket(wid)
			sh.state = "dropWorkerEnd"
		case act := <-sh.storageNotifier:
			sh.state = "storageNotifierStart"
			sh.onStorageNotify(act)
			sh.state = "storageNotifierEnd"
		case param := <-sh.droppedWorker:
			sh.state = "droppedWorkerStart"
			sh.onWorkerDropped(param)
			sh.state = "droppedWorkerEnd"
		case clean := <-sh.taskCleaner:
			sh.state = "taskCleanerStart"
			sh.onTaskClean(clean)
			sh.state = "taskCleanerEnd"
		case param := <-sh.workerStatsQuery:
			sh.state = "workerStatsQueryStart"
			sh.onCmdWorkerStatsQuery(param)
			sh.state = "workerStatsQueryEnd"
		case param := <-sh.workerJobsQuery:
			sh.state = "workerJobsQueryStart"
			sh.onCmdWorkerJobsQuery(param)
			sh.state = "workerJobsQueryEnd"
		case param := <-sh.bucketPledgedJobs:
			sh.state = "bucketPledgedJobsStart"
			sh.onBucketPledgedJobs(param)
			sh.state = "bucketPledgedJobsEnd"
		case uuid := <-sh.taskUUID:
			sh.state = "taskUUIDStart"
			sh.onTaskUUID(uuid)
			sh.state = "taskUUIDEnd"
		case sector := <-sh.abortTask:
			sh.state = "abortTaskStart"
			sh.onAbortTask(sector)
			sh.state = "abortTaskEnd"
		case workerMode := <-sh.setWorkerMode:
			sh.state = "setWorkerModeStart"
			sh.onSetWorkerMode(workerMode)
			sh.state = "setWorkerModeEnd"
		case workerReservedSpace := <-sh.setWorkerReservedSpace:
			sh.state = "setWorkerReservedSpaceStart"
			sh.onSetWorkerReservedSpace(workerReservedSpace)
			sh.state = "setWorkerReservedSpaceEnd"
		case <-sh.statsTicker.C:
			sh.state = "statsTickerStart"
			sh.onStatsTick()
			sh.state = "statsTickerEnd"
		case jobs := <-sh.workerJobsResp:
			sh.state = "workerJobsRespStart"
			sh.onWorkerJobsResp(jobs)
			sh.state = "workerJobsRespEnd"
		case stats := <-sh.workerStatsResp:
			sh.state = "workerJobsStatsStart"
			sh.onWorkerStatsResp(stats)
			sh.state = "workerJobsStatsEnd"
		case <-sh.closing:
			sh.state = "closingStart"
			sh.closeAllBuckets()
			sh.state = "closingEnd"
			return
		}
	}
}

func (sh *edispatcher) Info(ctx context.Context) (interface{}, error) {
	return nil, nil
}

func (sh *edispatcher) Close(ctx context.Context) error {
	go func() { sh.closing <- struct{}{} }()
	return nil
}

func (sh *edispatcher) NewWorker(w *eWorkerHandle) {
	w.wIndex = sh.nextWorker
	sh.nextWorker += 1
	go func() { sh.newWorker <- w }()
}

func (sh *edispatcher) WorkerStats() map[uuid.UUID]storiface.WorkerStats {
	resp := make(chan map[uuid.UUID]storiface.WorkerStats)
	param := &eWorkerStatsParam{
		command: eschedWorkerStats,
		resp:    resp,
	}
	go func() { sh.workerStatsQuery <- param }()
	select {
	case out := <-resp:
		return out
	}
}

func (sh *edispatcher) WorkerJobs() map[uuid.UUID][]storiface.WorkerJob {
	resp := make(chan map[uuid.UUID][]storiface.WorkerJob)
	param := &eWorkerJobsParam{
		command: eschedWorkerJobs,
		resp:    resp,
	}
	go func() { sh.workerJobsQuery <- param }()
	select {
	case out := <-resp:
		return out
	}
}

func (sh *edispatcher) doCleanTask(sector storage.SectorRef, taskType sealtasks.TaskType, stage string, byCacheMove bool, bySectorRemove bool) {
	clean, ok := eschedTaskCleanMap[taskType]
	if !ok {
		return
	}

	if clean.stage != stage {
		return
	}

	sh.taskCleaner <- &eWorkerTaskCleaning{
		sector:         sector,
		taskType:       clean.taskType,
		byCacheMove:    byCacheMove,
		bySectorRemove: bySectorRemove,
	}
}

func (sh *edispatcher) MoveCacheDone(sector storage.SectorRef) {
	go sh.doCleanTask(sector, sealtasks.TTCommit2, eschedWorkerCleanAtFinish, true, false)
}

func (sh *edispatcher) RemoveSector(sector storage.SectorRef) {
	go sh.doCleanTask(sector, sealtasks.TTCommit2, eschedWorkerCleanAtFinish, false, true)
}

func (sh *edispatcher) Debugging() bool {
	return eschedDebug
}

func (sh *edispatcher) SetTaskUUID(sector storage.SectorRef, uuid uuid.UUID) {
	go func() { sh.taskUUID <- eTaskUUID{sector: sector, uuid: uuid} }()
}

func (sh *edispatcher) AbortTask(sector storage.SectorRef) error {
	go func() { sh.abortTask <- sector }()
	return nil
}

func (sh *edispatcher) SetScheduleConcurrent(idleCpus int, usableCpus int, apConcurrent int) error {
	for _, bucket := range sh.buckets {
		if 0 < idleCpus {
			bucket.idleCpus = idleCpus
		}
		if 0 < usableCpus {
			bucket.usableCpus = usableCpus
		}
		if 0 < apConcurrent {
			bucket.concurrentAP = apConcurrent
		}
	}
	return nil
}

func (sh *edispatcher) SetScheduleGpuConcurrentTasks(gpuTasks int) error {
	for _, bucket := range sh.buckets {
		if 0 < gpuTasks {
			bucket.gpuTasks = gpuTasks
		}
	}
	return nil
}

func (sh *edispatcher) SetWorkerMode(address string, mode string) error {
	go func() { sh.setWorkerMode <- eWorkerMode{address: address, mode: mode} }()
	return nil
}

func (sh *edispatcher) SetWorkerReservedSpace(address string, storePrefix string, reserved int64) error {
	go func() {
		sh.setWorkerReservedSpace <- eWorkerReservedSpace{
			address:     address,
			reserved:    reserved,
			storePrefix: storePrefix,
		}
	}()
	return nil
}

func (sh *edispatcher) PledgedJobs() int {
	resp := make(chan int)
	param := &eBucketPledgedJobsParam{
		resp: resp,
	}
	go func() { sh.bucketPledgedJobs <- param }()
	select {
	case jobs := <-resp:
		return jobs
	}
}
