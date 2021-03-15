package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	proof2 "github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"golang.org/x/xerrors"
)

var playAsMaster = false

type SlaveProver struct {
	addr    string
	nodeApi api.StorageMiner
	closer  jsonrpc.ClientCloser
}

type PoStScheduler struct {
	MasterProver string
	slaveProver  map[string]*SlaveProver
	newProver    chan *SlaveProver
}

func NewPoStScheduler() *PoStScheduler {
	sched := &PoStScheduler{
		slaveProver: make(map[string]*SlaveProver),
		newProver:   make(chan *SlaveProver, 10),
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

func (s *PoStScheduler) schedule() {
	for {
		select {
		case prover := <-s.newProver:
			if _, ok := s.slaveProver[prover.addr]; !ok {
				log.Infof("new prover: %v", prover.addr)
				s.slaveProver[prover.addr] = prover
			}
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
	return nil, nil, xerrors.Errorf("REMOTE POST IS NOT IMPLEMENTED")
}
