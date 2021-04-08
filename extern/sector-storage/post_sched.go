package sectorstorage

import (
	"context"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	proof2 "github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"os"
	"time"
)

var playAsMaster = false

type SlaveProver struct {
	addr           string
	nodeApi        api.StorageMiner
	closer         jsonrpc.ClientCloser
	running        bool
	heartbeatFails int
}

const postExpectFinishedTime = 8 * time.Minute
const postWorstFinishedTime = 30 * time.Minute
const postSlaveProverHeartbeatInterval = 5 * time.Second

type postTask struct {
	ctx        context.Context
	minerID    abi.ActorID
	sectorInfo []proof2.SectorInfo
	randomness abi.PoStRandomness
	taskId     uint64
	dueTime    time.Time
	worstTime  time.Time
	ret        chan *postTaskOutput
	scheduled  bool
}

type postTaskOutput struct {
	taskId  uint64
	proofs  []proof2.PoStProof
	sectors []abi.SectorID
	err     error
}

type slaveCheckInput struct {
	addr string
	resp chan bool
}

type PoStScheduler struct {
	MasterProver  string
	slaveProver   map[string]*SlaveProver
	newProver     chan *SlaveProver
	slaveChecker  chan slaveCheckInput
	newTask       chan *postTask
	taskFinished  chan *postTaskOutput
	sectorProving chan SectorProvingInput
	scheduleTask  chan struct{}
	postTasks     []*postTask
	currentId     uint64
}

type SectorProvingInput struct {
	sector storage.SectorRef
	paths  []stores.SectorStorageInfo
}

func NewPoStScheduler() *PoStScheduler {
	sched := &PoStScheduler{
		slaveProver:   make(map[string]*SlaveProver),
		newProver:     make(chan *SlaveProver, 10),
		slaveChecker:  make(chan slaveCheckInput, 10),
		newTask:       make(chan *postTask, 20),
		taskFinished:  make(chan *postTaskOutput, 20),
		sectorProving: make(chan SectorProvingInput, 20),
		scheduleTask:  make(chan struct{}, 20),
		postTasks:     make([]*postTask, 0),
	}

	go sched.schedule()

	return sched
}

func (s *PoStScheduler) AddSlaveProver(addr string, nodeApi api.StorageMiner, closer jsonrpc.ClientCloser) {
	go func() {
		log.Infof("add slave prover %v", addr)
		s.newProver <- &SlaveProver{
			addr:    addr,
			nodeApi: nodeApi,
			closer:  closer,
		}
	}()
}

func (s *PoStScheduler) SetMasterProver(addr string) {
	s.MasterProver = addr
}

func (s *PoStScheduler) GetMasterProver() string {
	return s.MasterProver
}

func (s *PoStScheduler) runTask(prover *SlaveProver, task *postTask) {
	go func() {
		log.Infof("run task %v start", task.taskId)
		output, err := prover.nodeApi.GenerateWindowPoSt(task.ctx, task.minerID, task.sectorInfo, task.randomness)
		log.Infof("run task %v done", task.taskId)
		prover.running = false

		s.taskFinished <- &postTaskOutput{
			taskId:  task.taskId,
			proofs:  output.Proofs,
			sectors: output.Sectors,
			err:     err,
		}
	}()
}

func (s *PoStScheduler) scheduleWaitQueue() {
	for _, task := range s.postTasks {
		if task.scheduled && time.Now().Before(task.dueTime) {
			continue
		}

		if time.Now().After(task.worstTime) {
			go func(task *postTask) {
				s.taskFinished <- &postTaskOutput{
					taskId:  task.taskId,
					proofs:  nil,
					sectors: nil,
					err:     xerrors.Errorf("task take too long time, or no post prover available"),
				}
			}(task)
			continue
		}

		scheduled := false

		for addr, prover := range s.slaveProver {
			gpus, _ := ffi.GetGPUDevices()
			if s.GetPlayAsMaster() && len(gpus) < 2 && 1 < len(s.slaveProver) {
				continue
			}

			if prover.running {
				continue
			}

			if 0 < prover.heartbeatFails {
				continue
			}

			prover.running = true
			task.scheduled = true
			task.dueTime = time.Now().Add(postExpectFinishedTime)

			log.Infof("run task %v by %v", task.taskId, addr)
			s.runTask(prover, task)

			break
		}

		if !scheduled {
			return
		}
	}
}

func (s *PoStScheduler) checkProverHeartbeat() {
	for addr, prover := range s.slaveProver {
		if prover.running {
			continue
		}

		ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
		defer cancel()

		err := prover.nodeApi.CheckMaster(ctx)
		if err != nil {
			log.Errorf("fail to check heartbeat to %v", addr)
			prover.heartbeatFails += 1
		}
		if 5 < prover.heartbeatFails {
			s.slaveProver[addr].closer()
			delete(s.slaveProver, addr)
		}
	}
}

func (s *PoStScheduler) notifySectorProving(sector storage.SectorRef, paths []stores.SectorStorageInfo) {
	for addr, prover := range s.slaveProver {
		log.Infof("notify sector %v proving to %v", sector, addr)
		err := prover.nodeApi.NotifySectorProving(context.TODO(), sector, paths)
		if err != nil {
			log.Errorf("fail to notify sector %v proving to %v", sector, addr)
		}
	}
}

func (s *PoStScheduler) schedule() {
	ticker := time.NewTicker(postExpectFinishedTime)
	checker := time.NewTicker(postSlaveProverHeartbeatInterval)

	for {
		select {
		case prover := <-s.newProver:
			if _, ok := s.slaveProver[prover.addr]; !ok {
				log.Infof("new prover: %v", prover.addr)
				s.slaveProver[prover.addr] = prover
			}
		case checker := <-s.slaveChecker:
			exists := false
			if _, ok := s.slaveProver[checker.addr]; ok {
				exists = true
			}
			go func(checkout slaveCheckInput) {
				checker.resp <- exists
			}(checker)
		case task := <-s.newTask:
			task.taskId = s.currentId
			s.currentId += 1
			s.postTasks = append(s.postTasks, task)
			go func() { s.scheduleTask <- struct{}{} }()
		case output := <-s.taskFinished:
			for idx, task := range s.postTasks {
				if task.taskId == output.taskId {
					s.postTasks = append(s.postTasks[0:idx], s.postTasks[idx+1:]...)
					task.ret <- output
					break
				}
			}
			go func() { s.scheduleTask <- struct{}{} }()
		case input := <-s.sectorProving:
			s.notifySectorProving(input.sector, input.paths)
		case <-s.scheduleTask:
			s.scheduleWaitQueue()
		case <-ticker.C:
			s.scheduleWaitQueue()
		case <-checker.C:
			s.checkProverHeartbeat()
		}
	}
}

func (s *PoStScheduler) SetPlayAsMaster(master bool, addr string) error {
	playAsMaster = master
	if master {
		s.MasterProver = addr
		os.Unsetenv("FFI_MULTIEXP_USE_ALL_GPU")
	} else {
		os.Setenv("FFI_MULTIEXP_USE_ALL_GPU", "1")
	}
	return nil
}

func (s *PoStScheduler) GetPlayAsMaster() bool {
	return playAsMaster
}

func (s *PoStScheduler) GenerateWindowPoSt(ctx context.Context, minerID abi.ActorID, sectorInfo []proof2.SectorInfo, randomness abi.PoStRandomness) ([]proof2.PoStProof, []abi.SectorID, error) {
	resp := make(chan *postTaskOutput)
	go func() {
		s.newTask <- &postTask{
			ctx:        ctx,
			minerID:    minerID,
			sectorInfo: sectorInfo,
			randomness: randomness,
			ret:        resp,
			dueTime:    time.Now().Add(postExpectFinishedTime),
			worstTime:  time.Now().Add(postWorstFinishedTime),
		}
	}()

	select {
	case output := <-resp:
		return output.proofs, output.sectors, output.err
	}

	return nil, nil, xerrors.Errorf("WE SHOULD NOT BE HERE")
}

func (s *PoStScheduler) SectorProving(ctx context.Context, sector storage.SectorRef, infos []stores.SectorStorageInfo) error {
	go func() {
		s.sectorProving <- SectorProvingInput{
			sector: sector,
			paths:  infos,
		}
	}()
	return nil
}

func (s *PoStScheduler) SlaveConnected(ctx context.Context, addr string) bool {
	resp := make(chan bool)
	go func() {
		s.slaveChecker <- slaveCheckInput{
			addr: addr,
			resp: resp,
		}
	}()

	select {
	case connected := <-resp:
		return connected
	}

	return false
}
