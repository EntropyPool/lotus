package sectorstorage

import (
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
)

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
