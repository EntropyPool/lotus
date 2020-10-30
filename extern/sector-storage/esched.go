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
	Memory    uint64
	CPUs      int
	GPUs      int
	DiskSpace int64
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
		abi.RegisteredSealProof_StackedDrg64GiBV1:  &eResources{Memory: 32 * 1024 * 1024 * 1024, CPUs: 2, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg32GiBV1:  &eResources{Memory: 16 * 1024 * 1024 * 1024, CPUs: 2, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg512MiBV1: &eResources{Memory: 1024 * 1024 * 1024, CPUs: 1, GPUs: 1, DiskSpace: 512 * eMiB},
		abi.RegisteredSealProof_StackedDrg2KiBV1:   &eResources{Memory: 4 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 2 * eMiB},
		abi.RegisteredSealProof_StackedDrg8MiBV1:   &eResources{Memory: 16 * 1024 * 1024, CPUs: 1, GPUs: 0, DiskSpace: 16 * eMiB},
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

type eWorkerReqList struct {
	taskType sealtasks.TaskType
	tasks    []*eWorkerRequest
}

type eWorkerHandle struct {
	priv       interface{}
	wid        WorkerID
	w          Worker
	wt         *workTracker
	info       storiface.WorkerInfo
	memUsed    uint64
	cpuUsed    int
	gpuUsed    int
	diskUsed   int64
	diskTotal  int64
	typedTasks map[sealtasks.TaskType]*eWorkerReqList
}

const eschedTag = "esched"

type eRequestFinisher struct {
	resp *workerResponse
	req  *eWorkerRequest
	wid  WorkerID
}

type eWorkerBucket struct {
	spt             abi.RegisteredSealProof
	id              int
	newWorker       chan *eWorkerHandle
	workers         []*eWorkerHandle
	reqQueue        *eRequestQueue
	schedulerWaker  chan struct{}
	reqFinisher     chan *eRequestFinisher
	notifier        chan struct{}
	dropWorker      chan WorkerID
	retRequest      chan *eWorkerRequest
	storageNotifier chan eStoreAction
	droppedWorker   chan string
}

type eRequestQueue struct {
	reqs  map[sealtasks.TaskType][]*eWorkerRequest
	mutex sync.Mutex
}

const eschedAdd = "add"
const eschedDrop = "drop"

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

type edispatcher struct {
	spt             abi.RegisteredSealProof
	nextWorker      WorkerID
	nextRequest     uint64
	newWorker       chan *eWorkerHandle
	dropWorker      chan WorkerID
	newRequest      chan *eWorkerRequest
	buckets         []*eWorkerBucket
	reqQueue        *eRequestQueue
	storage         *EStorage
	storageNotifier chan eStoreAction
	droppedWorker   chan string
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

func (w *eWorkerHandle) acquireRequestResource(req *eWorkerRequest) {
	w.cpuUsed += req.cpuUsed
	w.gpuUsed += req.gpuUsed
	w.memUsed += req.memUsed
	w.diskUsed += req.diskUsed
}

func (w *eWorkerHandle) releaseRequestResource(req *eWorkerRequest) {
	w.cpuUsed -= req.cpuUsed
	w.gpuUsed -= req.gpuUsed
	w.memUsed -= req.memUsed
	w.diskUsed -= req.diskUsed
}

func (bucket *eWorkerBucket) tryPeekAsManyRequests(worker *eWorkerHandle, taskType sealtasks.TaskType, reqQueue *eRequestQueue) int {
	log.Debugf("<%s> peek as many request to worker", eschedTag)
	worker.dumpWorkerInfo()
	log.Debugf("<%s> %v need following resource", eschedTag, taskType)
	res := findTaskResource(bucket.spt, taskType)
	res.dumpResources()

	idleCpus := int(worker.info.Resources.CPUs * 8 / 100)
	reqs := reqQueue.reqs[taskType]
	log.Debugf("<%s> %d %v tasks waiting for run", eschedTag, len(reqs), taskType)

	peekReqs := 0
	for {
		if 0 == len(reqs) {
			break
		}
		if int(worker.info.Resources.CPUs)-idleCpus <= res.CPUs+worker.cpuUsed {
			break
		}
		if 0 < res.GPUs {
			if len(worker.info.Resources.GPUs) <= res.GPUs+worker.gpuUsed {
				break
			}
		}
		if worker.info.Resources.MemPhysical <= res.Memory+worker.memUsed {
			break
		}
		// TODO: disk space need to be added

		req := reqs[0]
		worker.typedTasks[taskType].tasks = append(worker.typedTasks[taskType].tasks, req)

		req.cpuUsed = res.CPUs
		req.gpuUsed = res.GPUs
		req.memUsed = res.Memory
		// TODO: if P1 and P2 is the different worker, we need to process it
		req.diskUsed = res.DiskSpace

		req.inqueueTime = time.Now().UnixNano()
		worker.acquireRequestResource(req)

		peekReqs += 1
		reqs = reqs[1:]
	}

	reqQueue.reqs[taskType] = reqs

	return peekReqs
}

func (bucket *eWorkerBucket) tryPeekRequest() {
	log.Debugf("<%s> try peek schedule request from %d workers of bucket [%d]", eschedTag, len(bucket.workers), bucket.id)

	peekReqs := 0

	for _, worker := range bucket.workers {
		for taskType, _ := range worker.typedTasks {
			bucket.reqQueue.mutex.Lock()
			if _, ok := bucket.reqQueue.reqs[taskType]; ok {
				if 0 < len(bucket.reqQueue.reqs[taskType]) {
					peekReqs += bucket.tryPeekAsManyRequests(worker, taskType, bucket.reqQueue)
				}
			}
			bucket.reqQueue.mutex.Unlock()
		}
	}

	if 0 < peekReqs {
		go func() { bucket.schedulerWaker <- struct{}{} }()
	}
}

func (bucket *eWorkerBucket) runTypedTask(worker *eWorkerHandle, task *eWorkerRequest) {
	log.Infof("<%s> run typed task %v/%v", eschedTag, task.sector, task.taskType)
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
	err = task.work(task.ctx, worker.wt.worker(worker.w))
	bucket.reqFinisher <- &eRequestFinisher{
		req:  task,
		resp: &workerResponse{err: err},
		wid:  worker.wid,
	}
	task.endTime = time.Now().UnixNano()
	log.Infof("<%s> finished typed task %v/%v", eschedTag, task.sector, task.taskType)
}

func (bucket *eWorkerBucket) scheduleTypedTasks(worker *eWorkerHandle) {
	for _, typedTasks := range worker.typedTasks {
		tasks := typedTasks.tasks
		for {
			if 0 == len(tasks) {
				break
			}
			task := tasks[0]
			go bucket.runTypedTask(worker, task)
			tasks = tasks[1:]
		}
		typedTasks.tasks = tasks
	}
}

func (bucket *eWorkerBucket) scheduleBucketTask() {
	log.Debugf("<%s> try schedule bucket task for bucket [%d]", eschedTag, bucket.id)
	for _, worker := range bucket.workers {
		bucket.scheduleTypedTasks(worker)
	}
}

func (bucket *eWorkerBucket) removeTaskFromBucketWorker(w *eWorkerHandle, req *eWorkerRequest) {
	taskIndex := -1
	var typedTasks *eWorkerReqList

	for _, tts := range w.typedTasks {
		for id, task := range tts.tasks {
			if task.id == req.id {
				taskIndex = id
				typedTasks = tts
				goto l_remove_task
			}
		}
	}

	if taskIndex < 0 {
		return
	}

	w.releaseRequestResource(req)

l_remove_task:
	typedTasks.tasks = append(typedTasks.tasks[:taskIndex], typedTasks.tasks[taskIndex+1:]...)
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
		bucket.removeTaskFromBucketWorker(w, finisher.req)
	}

	go func() { finisher.req.ret <- *finisher.resp }()
	go func() { bucket.notifier <- struct{}{} }()
	go func() { bucket.schedulerWaker <- struct{}{} }()
}

func (bucket *eWorkerBucket) removeWorkerFromBucket(wid WorkerID) {
	log.Infof("<%s> try to drop worker %v from bucket %d", eschedTag, wid, bucket.id)
	worker := bucket.findBucketWorkerByID(wid)
	if nil != worker {
		for _, typedTasks := range worker.typedTasks {
			for _, task := range typedTasks.tasks {
				go func() { bucket.retRequest <- task }()
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
	w.diskTotal += act.stat.space
}

func (bucket *eWorkerBucket) onDropStore(w *eWorkerHandle, act eStoreAction) {
	w.diskTotal -= act.stat.space
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
		case finisher := <-bucket.reqFinisher:
			bucket.taskFinished(finisher)
		case wid := <-bucket.dropWorker:
			bucket.removeWorkerFromBucket(wid)
		case act := <-bucket.storageNotifier:
			bucket.onStorageNotify(act)
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
	}

	for i := range dispatcher.buckets {
		dispatcher.buckets[i] = &eWorkerBucket{
			spt:             spt,
			id:              i,
			newWorker:       make(chan *eWorkerHandle),
			workers:         make([]*eWorkerHandle, 0),
			reqQueue:        dispatcher.reqQueue,
			schedulerWaker:  make(chan struct{}, 20),
			reqFinisher:     make(chan *eRequestFinisher),
			notifier:        make(chan struct{}),
			dropWorker:      make(chan WorkerID, 10),
			retRequest:      dispatcher.newRequest,
			storageNotifier: make(chan eStoreAction, 10),
			droppedWorker:   dispatcher.droppedWorker,
		}
		go dispatcher.buckets[i].scheduler()
	}

	eResourceTable[sealtasks.TTUnseal] = eResourceTable[sealtasks.TTPreCommit1]
	eResourceTable[sealtasks.TTReadUnsealed] = eResourceTable[sealtasks.TTFetch]

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

func (sh *edispatcher) addNewWorkerToBucket(w *eWorkerHandle) {
	w.patchLocalhost()

	log.Infof("<%s> add new worker %s to bucket", eschedTag, w.info.Address)
	w.dumpWorkerInfo()

	w.typedTasks = make(map[sealtasks.TaskType]*eWorkerReqList)
	for _, taskType := range w.info.SupportTasks {
		w.typedTasks[taskType] = &eWorkerReqList{
			taskType: taskType,
			tasks:    make([]*eWorkerRequest, 0),
		}
	}

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
	sh.storage.workerNotifier <- eWorkerAction{act: eschedAdd, address: address}
}

func (sh *edispatcher) runSched() {
	for {
		select {
		case req := <-sh.newRequest:
			sh.addNewWorkerRequestToBucketWorker(req)
		case w := <-sh.newWorker:
			sh.addNewWorkerToBucket(w)
		case wid := <-sh.dropWorker:
			sh.dropWorkerFromBucket(wid)
		case act := <-sh.storageNotifier:
			sh.onStorageNotify(act)
		case address := <-sh.droppedWorker:
			sh.onWorkerDropped(address)
		}
	}
}

func (sh *edispatcher) Info(ctx context.Context) (interface{}, error) {
	return nil, nil
}

func (sh *edispatcher) Close(ctx context.Context) error {
	return nil
}

func (sh *edispatcher) NewWorker(w *eWorkerHandle) {
	w.wid = sh.nextWorker
	sh.nextWorker += 1
	sh.newWorker <- w
}

func (w *eWorkerHandle) WID() WorkerID {
	return w.wid
}

func (sh *edispatcher) DropWorker(wid WorkerID) {
	log.Infof("------------------drop----- %v", wid)
	sh.dropWorker <- wid
}
