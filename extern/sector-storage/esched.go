package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"sync"
)

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
	wid        WorkerID
	w          Worker
	info       storiface.WorkerInfo
	typedTasks map[sealtasks.TaskType]*eWorkerReqList
}

const eschedTag = "esched"

type eRequestFinisher struct {
	resp workerResponse
	req  *eWorkerRequest
}

type eWorkerBucket struct {
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

func (bucket *eWorkerBucket) peekAsManyRequests(worker *eWorkerHandle, taskType sealtasks.TaskType, reqs []*eWorkerRequest) {
	log.Debugf("<%s> peek as many request to worker")
	worker.dumpWorkerInfo()
}

func (bucket *eWorkerBucket) tryPeekRequest() {
	log.Debugf("<%s> try peek schedule request from %d workers of bucket [%d]", eschedTag, len(bucket.workers), bucket.id)
	for _, worker := range bucket.workers {
		for taskType, _ := range worker.typedTasks {
			bucket.reqQueue.mutex.Lock()
			if _, ok := bucket.reqQueue.reqs[taskType]; ok {
				if 0 < len(bucket.reqQueue.reqs[taskType]) {
					bucket.peekAsManyRequests(worker, taskType, bucket.reqQueue.reqs[taskType])
				}
			}
			bucket.reqQueue.mutex.Unlock()
		}
	}
}

func (bucket *eWorkerBucket) scheduleBucketTask() {
	log.Debugf("<%s> try schedule bucket task", eschedTag)
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
			finisher.req.ret <- finisher.resp
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
			id:             i,
			newWorker:      make(chan *eWorkerHandle),
			workers:        make([]*eWorkerHandle, 0),
			reqQueue:       dispatcher.reqQueue,
			schedulerWaker: make(chan struct{}),
			reqFinisher:    make(chan *eRequestFinisher),
			notifier:       make(chan struct{}),
		}
		go dispatcher.buckets[i].scheduler()
	}

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
	log.Infof("  Mem Swap: ------------------ %v", w.info.Resources.MemSwap)
	log.Infof("  Mem Reserved: -------------- %v", w.info.Resources.MemReserved)
	log.Infof("  CPUs: ---------------------- %v", w.info.Resources.CPUs)
	log.Infof("  GPUs------------------------")
	for _, gpu := range w.info.Resources.GPUs {
		log.Infof("    + %s", gpu)
	}
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
