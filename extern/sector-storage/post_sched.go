package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	proof2 "github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"golang.org/x/xerrors"
	"time"
)

var playAsMaster = false

type SlaveProver struct {
	addr    string
	nodeApi api.StorageMiner
	closer  jsonrpc.ClientCloser
	running bool
}

const postExpectFinishedTime = 8 * time.Minute
const postWorstFinishedTime = 30 * time.Minute

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

type PoStScheduler struct {
	MasterProver string
	slaveProver  map[string]*SlaveProver
	newProver    chan *SlaveProver
	newTask      chan *postTask
	taskFinished chan *postTaskOutput
	scheduleTask chan struct{}
	postTasks    []*postTask
	currentId    uint64
}

func NewPoStScheduler() *PoStScheduler {
	sched := &PoStScheduler{
		slaveProver:  make(map[string]*SlaveProver),
		newProver:    make(chan *SlaveProver, 10),
		newTask:      make(chan *postTask, 20),
		taskFinished: make(chan *postTaskOutput, 20),
		scheduleTask: make(chan struct{}, 20),
		postTasks:    make([]*postTask, 0),
	}

	go sched.schedule()

	return sched
}

func (s *PoStScheduler) AddSlaveProver(addr string, nodeApi api.StorageMiner, closer jsonrpc.ClientCloser) {
	go func() {
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
		output, err := prover.nodeApi.GenerateWindowPoSt(task.ctx, task.minerID, task.sectorInfo, task.randomness)
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
			if prover.running {
				continue
			}

			prover.running = true
			task.scheduled = true

			log.Infof("run task %v by %v", task.taskId, addr)
			s.runTask(prover, task)
		}

		if !scheduled {
			return
		}
	}
}

func (s *PoStScheduler) schedule() {
	ticker := time.NewTicker(postExpectFinishedTime)
	for {
		select {
		case prover := <-s.newProver:
			if _, ok := s.slaveProver[prover.addr]; !ok {
				log.Infof("new prover: %v", prover.addr)
				s.slaveProver[prover.addr] = prover
			}
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
			s.scheduleTask <- struct{}{}
		case <-s.scheduleTask:
			s.scheduleWaitQueue()
		case <-ticker.C:
			s.scheduleWaitQueue()
		}
	}
}

func (s *PoStScheduler) SetPlayAsMaster(master bool, addr string) error {
	playAsMaster = master
	if master {
		s.MasterProver = addr
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
