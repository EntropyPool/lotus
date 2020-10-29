package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"sync"
)

type eResources struct {
	Memory    uint64
	CPUs      int
	GPUs      int
	DiskSpace uint64
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
	sector   abi.SectorID
	taskType sealtasks.TaskType
	prepare  WorkerAction
	work     WorkerAction
	ret      chan workerResponse
	ctx      context.Context
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
	diskUsed   uint64
	diskTotal  uint64 // TODO: set disk total from store
	typedTasks map[sealtasks.TaskType]*eWorkerReqList
}

const eschedTag = "esched"

type eRequestFinisher struct {
	resp *workerResponse
	req  *eWorkerRequest
	wid  WorkerID
}

type eWorkerBucket struct {
	spt            abi.RegisteredSealProof
	id             int
	newWorker      chan *eWorkerHandle
	workers        []*eWorkerHandle
	reqQueue       *eRequestQueue
	schedulerWaker chan struct{}
	reqFinisher    chan *eRequestFinisher
	notifier       chan struct{}
}

type eRequestQueue struct {
	reqs  map[sealtasks.TaskType][]*eWorkerRequest
	mutex sync.Mutex
}

type edispatcher struct {
	spt        abi.RegisteredSealProof
	nextWorker WorkerID
	newWorker  chan *eWorkerHandle
	dropWorker chan WorkerID
	newRequest chan *eWorkerRequest
	buckets    []*eWorkerBucket
	reqQueue   *eRequestQueue
}

const eschedWorkerBuckets = 10

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

		worker.typedTasks[taskType].tasks = append(worker.typedTasks[taskType].tasks, reqs[0])
		reqs = reqs[1:]

		worker.cpuUsed += res.CPUs
		worker.gpuUsed += res.GPUs
		worker.memUsed += res.Memory

		// TODO: if P1 and P2 is the different worker, we need to process it
		worker.diskUsed += res.DiskSpace

		peekReqs += 1
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
	log.Debugf("<%s> run typed task")
	worker.dumpWorkerInfo()
	task.dumpWorkerRequest()

	err := task.prepare(task.ctx, worker.wt.worker(worker.w))
	if nil != err {
		bucket.reqFinisher <- &eRequestFinisher{
			req:  task,
			resp: &workerResponse{err: err},
			wid:  worker.wid,
		}
		return
	}
	err = task.work(task.ctx, worker.wt.worker(worker.w))
	bucket.reqFinisher <- &eRequestFinisher{
		req:  task,
		resp: &workerResponse{err: err},
		wid:  worker.wid,
	}
}

func (bucket *eWorkerBucket) scheduleTypedTasks(worker *eWorkerHandle) {
	for _, typedTasks := range worker.typedTasks {
		for _, task := range typedTasks.tasks {
			go bucket.runTypedTask(worker, task)
		}
	}
}

func (bucket *eWorkerBucket) scheduleBucketTask() {
	log.Debugf("<%s> try schedule bucket task for bucket [%d]", eschedTag, bucket.id)
	for _, worker := range bucket.workers {
		bucket.scheduleTypedTasks(worker)
	}
}

func (bucket *eWorkerBucket) scheduler() {
	log.Infof("<%s> run scheduler for bucket %d", eschedTag, bucket.id)

	for {
		select {
		case w := <-bucket.newWorker:
			bucket.workers = append(bucket.workers, w)
			log.Infof("<%s> new worker [%v] %s is added to bucket %d [%d]",
				eschedTag, w.wid, w.info.Address, bucket.id, len(bucket.workers))
		case <-bucket.notifier:
			bucket.tryPeekRequest()
		case <-bucket.schedulerWaker:
			bucket.scheduleBucketTask()
		case finisher := <-bucket.reqFinisher:
			go func() { finisher.req.ret <- *finisher.resp }()
			go func() { bucket.notifier <- struct{}{} }()
			go func() { bucket.schedulerWaker <- struct{}{} }()
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
	}

	for i := range dispatcher.buckets {
		dispatcher.buckets[i] = &eWorkerBucket{
			spt:            spt,
			id:             i,
			newWorker:      make(chan *eWorkerHandle),
			workers:        make([]*eWorkerHandle, 0),
			reqQueue:       dispatcher.reqQueue,
			schedulerWaker: make(chan struct{}, 20),
			reqFinisher:    make(chan *eRequestFinisher),
			notifier:       make(chan struct{}),
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
		w.info.Address = "localhost"
		w.info.Hostname = "localhost"
		w.info.GroupName = "localhost"
	}
}

func (sh *edispatcher) addNewWorkerToBucket(w *eWorkerHandle) {
	w.patchLocalhost()

	log.Infof("<%s> add new worker to bucket", eschedTag)
	w.dumpWorkerInfo()

	w.wid = sh.nextWorker
	w.typedTasks = make(map[sealtasks.TaskType]*eWorkerReqList)
	for _, taskType := range w.info.SupportTasks {
		w.typedTasks[taskType] = &eWorkerReqList{
			taskType: taskType,
			tasks:    make([]*eWorkerRequest, 0),
		}
	}

	workerBucketIndex := sh.nextWorker % eschedWorkerBuckets
	bucket := sh.buckets[workerBucketIndex]

	bucket.addNewWorker(w)
}

func (req *eWorkerRequest) dumpWorkerRequest() {
	log.Debugf("Task Information -------")
	log.Debugf("  Sector ID: --------------- %v", req.sector)
	log.Debugf("  Task Type: --------------- %v", req.taskType)
}

func (sh *edispatcher) addNewWorkerRequestToBucketWorker(req *eWorkerRequest) {
	log.Infof("<%s> add new request to bucket worker", eschedTag)
	req.dumpWorkerRequest()

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
	log.Infof("<%s> drop worker from bucket %v", eschedTag, wid)
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
	sh.newWorker <- w
}

func (sh *edispatcher) DropWorker(wid WorkerID) {
	sh.dropWorker <- wid
}
