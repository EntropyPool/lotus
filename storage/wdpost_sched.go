package storage

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/node/config"

	"go.opencensus.io/trace"
)

type WindowPoStScheduler struct {
	api              storageMinerApi
	mainApi          storageMinerApi
	feeCfg           config.MinerFeeConfig
	addrSel          *AddressSelector
	prover           storage.Prover
	verifier         ffiwrapper.Verifier
	sealer           sectorstorage.SectorManager
	faultTracker     sectorstorage.FaultTracker
	proofType        abi.RegisteredPoStProof
	partitionSectors uint64
	ch               *changeHandler

	actor address.Address

	evtTypes [4]journal.EventType
	journal  journal.Journal

	chainEndpointersFetcher func(context.Context) (map[string]http.Header, error)
	wdpostCheckerListener   func(context.Context) (chan uint64, chan func() ([]miner.SubmitWindowedPoStParams, error))

	// failed abi.ChainEpoch // eps
	// failLk sync.Mutex
}

func NewWindowedPoStScheduler(api storageMinerApi, fc config.MinerFeeConfig, as *AddressSelector, sb storage.Prover, verif ffiwrapper.Verifier, ft sectorstorage.FaultTracker, j journal.Journal, actor address.Address) (*WindowPoStScheduler, error) {
	mi, err := api.StateMinerInfo(context.TODO(), actor, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("getting sector size: %w", err)
	}

	return &WindowPoStScheduler{
		api:              api,
		mainApi:          api,
		feeCfg:           fc,
		addrSel:          as,
		prover:           sb,
		verifier:         verif,
		faultTracker:     ft,
		proofType:        mi.WindowPoStProofType,
		partitionSectors: mi.WindowPoStPartitionSectors,

		actor: actor,
		evtTypes: [...]journal.EventType{
			evtTypeWdPoStScheduler:  j.RegisterEventType("wdpost", "scheduler"),
			evtTypeWdPoStProofs:     j.RegisterEventType("wdpost", "proofs_processed"),
			evtTypeWdPoStRecoveries: j.RegisterEventType("wdpost", "recoveries_processed"),
			evtTypeWdPoStFaults:     j.RegisterEventType("wdpost", "faults_processed"),
		},
		journal: j,
	}, nil
}

func (s *WindowPoStScheduler) SetSealer(sealer sectorstorage.SectorManager) {
	s.sealer = sealer
}

func (s *WindowPoStScheduler) SetChainEndpointsFetcher(fetcher func(ctx context.Context) (map[string]http.Header, error)) {
	s.chainEndpointersFetcher = fetcher
}

func (s *WindowPoStScheduler) SetWindowPoStCheckerListener(listener func(ctx context.Context) (chan uint64, chan func() ([]miner.SubmitWindowedPoStParams, error))) {
	s.wdpostCheckerListener = listener
}

type changeHandlerAPIImpl struct {
	storageMinerApi
	*WindowPoStScheduler
}

func (s *WindowPoStScheduler) chainNotify(ctx context.Context) (<-chan []*api.HeadChange, error) {
	ch, err := s.mainApi.ChainNotify(ctx)
	if err == nil {
		s.api = s.mainApi
		return ch, nil
	}

	log.Errorf("ChainNotify error: %v", err)

	endpoints, err := s.chainEndpointersFetcher(ctx)
	if err != nil {
		log.Errorf("fail to get chain endpoints: %v", err)
		return nil, err
	}

	for addr, headers := range endpoints {
		api, closer, err := client.NewFullNodeRPC(ctx, addr, headers)
		if err != nil {
			log.Errorf("fail to watch chain endpoint %v: %v", addr, err)
			continue
		}
		defer closer()
		ch, err = api.ChainNotify(ctx)
		if err == nil {
			s.api = api
			return ch, nil
		}
		log.Errorf("ChainNotify error from %v: %v", addr, err)
	}

	return nil, xerrors.Errorf("cannot find suitable fullnode")
}

func (s *WindowPoStScheduler) checkWindowPoSt(ctx context.Context, deadline uint64, wdpostResult chan func() ([]miner.SubmitWindowedPoStParams, error)) {
	go func() {
		log.Warnf("CHECKING WINDOW POST ----- %v", deadline)
		posts, err := s.runPost(ctx, dline.Info{
			Index:                deadline,
			WPoStPeriodDeadlines: miner.WPoStPeriodDeadlines,
		}, nil)
		log.Warnf("CHECKED WINDOW POST ----- %v", deadline)
		wdpostResult <- (func() ([]miner.SubmitWindowedPoStParams, error) {
			return posts, err
		})
	}()
}

func (s *WindowPoStScheduler) Run(ctx context.Context) {
	// Initialize change handler
	chImpl := &changeHandlerAPIImpl{storageMinerApi: s.api, WindowPoStScheduler: s}
	s.ch = newChangeHandler(chImpl, s.actor)
	defer s.ch.shutdown()
	s.ch.start()

	var notifs <-chan []*api.HeadChange
	var err error
	var gotCur bool

	wdpostChecker, wdpostResult := s.wdpostCheckerListener(ctx)

	// not fine to panic after this point
	for {
		select {
		case index := <-wdpostChecker:
			s.checkWindowPoSt(ctx, index, wdpostResult)
		default:
		}

		if !s.sealer.GetPlayAsMaster(ctx) {
			log.Infof("I'm not master, do not process window post")
			build.Clock.Sleep(10 * time.Second)
			continue
		}

		if notifs == nil {
			notifs, err = s.chainNotify(ctx)
			if err != nil {
				log.Errorf("ChainNotify error: %+v", err)

				build.Clock.Sleep(10 * time.Second)
				continue
			}

			gotCur = false
		}

		select {
		case changes, ok := <-notifs:
			if !ok {
				log.Warn("window post scheduler notifs channel closed")
				notifs = nil
				continue
			}

			if !gotCur {
				if len(changes) != 1 {
					log.Errorf("expected first notif to have len = 1")
					continue
				}
				chg := changes[0]
				if chg.Type != store.HCCurrent {
					log.Errorf("expected first notif to tell current ts")
					continue
				}

				ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.headChange")

				s.update(ctx, nil, chg.Val)

				span.End()
				gotCur = true
				continue
			}

			ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.headChange")

			var lowest, highest *types.TipSet = nil, nil

			for _, change := range changes {
				if change.Val == nil {
					log.Errorf("change.Val was nil")
				}
				switch change.Type {
				case store.HCRevert:
					lowest = change.Val
				case store.HCApply:
					highest = change.Val
				}
			}

			s.update(ctx, lowest, highest)

			span.End()
		case <-ctx.Done():
			return
		}
	}
}

func (s *WindowPoStScheduler) update(ctx context.Context, revert, apply *types.TipSet) {
	if apply == nil {
		log.Error("no new tipset in window post WindowPoStScheduler.update")
		return
	}
	err := s.ch.update(ctx, revert, apply)
	if err != nil {
		log.Errorf("handling head updates in window post sched: %+v", err)
	}
}

// onAbort is called when generating proofs or submitting proofs is aborted
func (s *WindowPoStScheduler) onAbort(ts *types.TipSet, deadline *dline.Info) {
	s.journal.RecordEvent(s.evtTypes[evtTypeWdPoStScheduler], func() interface{} {
		c := evtCommon{}
		if ts != nil {
			c.Deadline = deadline
			c.Height = ts.Height()
			c.TipSet = ts.Cids()
		}
		return WdPoStSchedulerEvt{
			evtCommon: c,
			State:     SchedulerStateAborted,
		}
	})
}

func (s *WindowPoStScheduler) getEvtCommon(err error) evtCommon {
	c := evtCommon{Error: err}
	currentTS, currentDeadline := s.ch.currentTSDI()
	if currentTS != nil {
		c.Deadline = currentDeadline
		c.Height = currentTS.Height()
		c.TipSet = currentTS.Cids()
	}
	return c
}
