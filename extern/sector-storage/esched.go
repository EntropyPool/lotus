package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type eWorkerRequest struct {
	sector   abi.SectorID
	taskType sealtasks.TaskType
	prepare  WorkerAction
	work     WorkerAction
	ctx      context.Context
}

type eWorkerReqList struct {
	tasks []*eWorkerRequest
}

type eWorkerHandle struct {
	wid        WorkerID
	w          Worker
	info       storiface.WorkerInfo
	typedTasks map[sealtasks.TaskType]eWorkerReqList
}

const eschedTag = "esched"

type eWorkerBucket struct {
	id        int
	newWorker chan *eWorkerHandle
	workers   []*eWorkerHandle
}

type edispatcher struct {
	spt        abi.RegisteredSealProof
	nextWorker WorkerID
	newWorker  chan *eWorkerHandle
	dropWorker chan WorkerID
	newRequest chan *eWorkerRequest
	buckets    []*eWorkerBucket
}

const eschedWorkerBuckets = 10

func (bucket *eWorkerBucket) scheduler() {
	log.Infof("run scheduler for bucket %d", bucket.id)

	for {
		select {
		case w := <-bucket.newWorker:
			bucket.workers = append(bucket.workers, w)
			log.Infof("<%s> new worker [%v] %s is added to bucket %d", eschedTag, w.wid, w.info.Address, bucket.id)
		}
	}

	log.Infof("end scheduler for bucket %d", bucket.id)
}

func (bucket *eWorkerBucket) addNewWorker(w *eWorkerHandle) {
	for _, worker := range bucket.workers {
		if w.info.Address == worker.info.Address {
			log.Warnf("<%s> worker %s already in bucket, should we remove it firstly?", eschedTag, worker.info.Address)
			return
		}
	}
	log.Infof("<%s> try to add worker [%d] %s to bucket %d", eschedTag, w.wid, w.info.Address, bucket.id)
	bucket.newWorker <- w
	log.Infof("<%s> success to add worker [%d] %s to bucket %d", eschedTag, w.wid, w.info.Address, bucket.id)
}

func newExtScheduler(spt abi.RegisteredSealProof) *edispatcher {
	dispatcher := &edispatcher{
		spt:        spt,
		nextWorker: 0,
		newWorker:  make(chan *eWorkerHandle, 40),
		dropWorker: make(chan WorkerID, 10),
		newRequest: make(chan *eWorkerRequest, 1000),
		buckets:    make([]*eWorkerBucket, eschedWorkerBuckets),
	}

	for i := range dispatcher.buckets {
		dispatcher.buckets[i] = &eWorkerBucket{
			id:        i,
			newWorker: make(chan *eWorkerHandle),
			workers:   make([]*eWorkerHandle, 0),
		}
		go dispatcher.buckets[i].scheduler()
	}

	return dispatcher
}

func (sh *edispatcher) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, prepare WorkerAction, work WorkerAction) error {
	select {
	case sh.newRequest <- &eWorkerRequest{
		sector:   sector,
		taskType: taskType,
		prepare:  prepare,
		work:     work,
		ctx:      ctx,
	}:
	}

	return nil
}

func (sh *edispatcher) dumpWorkerInfo(w *eWorkerHandle) {
	log.Infof("Worker information -----------")
	log.Infof("  Address: ------------------- %v", w.info.Address)
	log.Infof("  Group: --------------------- %v", w.info.GroupName)
	log.Infof("  Support Tasks: -------------")
	for _, taskType := range w.info.SupportTasks {
		log.Infof("    + %v", taskType)
	}
	log.Infof("  Hostname: ------------------ %v", w.info.Hostname)
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
	sh.dumpWorkerInfo(w)

	w.wid = sh.nextWorker
	workerBucketIndex := sh.nextWorker % eschedWorkerBuckets
	bucket := sh.buckets[workerBucketIndex]

	bucket.addNewWorker(w)
}

func (sh *edispatcher) addNewWorkerRequestToBucketList(req *eWorkerRequest) {
	log.Info("esched", "new request", req)
}

func (sh *edispatcher) dropWorkerFromBucket(wid WorkerID) {
	log.Info("esched", "drop worker", wid)
}

func (sh *edispatcher) runSched() {
	for {
		select {
		case req := <-sh.newRequest:
			sh.addNewWorkerRequestToBucketList(req)
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
