package sectorstorage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

var log = logging.Logger("advmgr")

var ErrNoWorkers = errors.New("no suitable workers found")

type URLs []string

type Worker interface {
	ffiwrapper.StorageSealer

	MoveStorage(ctx context.Context, sector abi.SectorID, types stores.SectorFileType) error

	Fetch(ctx context.Context, s abi.SectorID, ft stores.SectorFileType, ptype stores.PathType, am stores.AcquireMode) error
	UnsealPiece(context.Context, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error
	ReadPiece(context.Context, io.Writer, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize) (bool, error)

	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)

	// Returns paths accessible to the worker
	Paths(context.Context) ([]stores.StoragePath, error)

	Info(context.Context) (storiface.WorkerInfo, error)

	// returns channel signalling worker shutdown
	Closing(context.Context) (<-chan struct{}, error)

	Close() error
}

type SectorManager interface {
	SectorSize() abi.SectorSize

	ReadPiece(context.Context, io.Writer, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error

	ffiwrapper.StorageSealer
	storage.Prover
	FaultTracker
}

type WorkerID uint64

type Manager struct {
	scfg *ffiwrapper.Config

	ls         stores.LocalStorage
	storage    *stores.Remote
	localStore *stores.Local
	remoteHnd  *stores.FetchHandler
	index      stores.SectorIndex

	sched *scheduler

	storage.Prover

	failSectors map[abi.SectorID]struct{}
}

type SealerConfig struct {
	ParallelFetchLimit int

	// Local worker config
	AllowAddPiece   bool
	AllowPreCommit1 bool
	AllowPreCommit2 bool
	AllowCommit     bool
	AllowUnseal     bool
}

type StorageAuth http.Header

func New(ctx context.Context, ls stores.LocalStorage, si stores.SectorIndex, cfg *ffiwrapper.Config, sc SealerConfig, urls URLs, sa StorageAuth) (*Manager, error) {
	lstor, err := stores.NewLocal(ctx, ls, si, urls)
	if err != nil {
		return nil, err
	}

	prover, err := ffiwrapper.New(&readonlyProvider{stor: lstor, index: si, spt: cfg.SealProofType}, cfg)
	if err != nil {
		return nil, xerrors.Errorf("creating prover instance: %v", err)
	}

	stor := stores.NewRemote(lstor, si, http.Header(sa), sc.ParallelFetchLimit)

	m := &Manager{
		scfg: cfg,

		ls:         ls,
		storage:    stor,
		localStore: lstor,
		remoteHnd:  &stores.FetchHandler{Local: lstor},
		index:      si,

		sched: newScheduler(cfg.SealProofType),

		Prover:      prover,
		failSectors: make(map[abi.SectorID]struct{}),
	}

	log.Infof("try to collect removable miner fail sector")
	failSectors := m.localStore.MayFailSectors
	for failSector, failInfo := range failSectors {
		sectorID := abi.SectorID{Miner: failInfo.Miner, Number: failSector}
		log.Infof("collect removable miner fail sector: %v", sectorID)
		m.failSectors[sectorID] = struct{}{}
	}

	m.sched.SetStorage(&EStorage{
		ctx:   ctx,
		index: si,
	})
	go m.sched.runSched()

	localTasks := []sealtasks.TaskType{
		sealtasks.TTFinalize, sealtasks.TTFetch, sealtasks.TTReadUnsealed,
	}
	if sc.AllowAddPiece {
		localTasks = append(localTasks, sealtasks.TTAddPiece)
	}
	if sc.AllowPreCommit1 {
		localTasks = append(localTasks, sealtasks.TTAddPiece)
		localTasks = append(localTasks, sealtasks.TTPreCommit1)
	}
	if sc.AllowPreCommit2 {
		localTasks = append(localTasks, sealtasks.TTPreCommit2)
		localTasks = append(localTasks, sealtasks.TTCommit1)
	}
	if sc.AllowCommit {
		localTasks = append(localTasks, sealtasks.TTCommit2)
	}
	if sc.AllowUnseal {
		localTasks = append(localTasks, sealtasks.TTUnseal)
	}

	err = m.AddWorker(ctx, NewLocalWorker(WorkerConfig{
		SealProof: cfg.SealProofType,
		TaskTypes: localTasks,
	}, stor, lstor, si))
	if err != nil {
		return nil, xerrors.Errorf("adding local worker: %v", err)
	}

	return m, nil
}

func (m *Manager) AddLocalStorage(ctx context.Context, path string) error {
	path, err := homedir.Expand(path)
	if err != nil {
		return xerrors.Errorf("expanding local path: %v", err)
	}

	if err := m.localStore.OpenPath(ctx, path); err != nil {
		return xerrors.Errorf("opening local path: %v", err)
	}

	if err := m.ls.SetStorage(func(sc *stores.StorageConfig) {
		sc.StoragePaths = append(sc.StoragePaths, stores.LocalPath{Path: path})
	}); err != nil {
		return xerrors.Errorf("get storage config: %v", err)
	}
	return nil
}

func (m *Manager) AddWorker(ctx context.Context, w Worker) error {
	info, err := w.Info(ctx)
	if err != nil {
		return xerrors.Errorf("getting worker info: %v", err)
	}

	log.Infof("add worker: %+v", info)
	m.sched.newWorkers <- &workerHandle{
		w: w,
		wt: &workTracker{
			running: map[uint64]storiface.WorkerJob{},
		},
		info:      info,
		preparing: &activeResources{},
		active:    &activeResources{},
	}

	timer := time.NewTimer(30 * time.Second)
	go func() {
		defer timer.Stop()
		select {
		case <-timer.C:
			failSectors := m.localStore.FailSectors
			for failSector, failInfo := range failSectors {
				sectorID := abi.SectorID{Miner: failInfo.Miner, Number: failSector}
				log.Infof("try to remove worker fail sector: %v", sectorID)
				err := m.Remove(ctx, sectorID)
				if nil != err {
					log.Errorf("cannot remove worker fail sector %v [%v]", sectorID, err)
				}
				// m.localStore.DropFailSector(ctx, sectorID)
			}
			for sectorID, _ := range m.failSectors {
				log.Infof("try to remove miner fail sector: %v", sectorID)
				err := m.Remove(ctx, sectorID)
				if nil != err {
					log.Errorf("cannot remove worker fail sector %v [%v]", sectorID, err)
				}
				// m.localStore.DropMayFailSector(ctx, sectorID)
				delete(m.failSectors, sectorID)
			}
		}
	}()

	return nil
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.remoteHnd.ServeHTTP(w, r)
}

func (m *Manager) SectorSize() abi.SectorSize {
	sz, _ := m.scfg.SealProofType.SectorSize()
	return sz
}

func schedNop(context.Context, Worker) error {
	return nil
}

func schedFetch(sector abi.SectorID, ft stores.SectorFileType, ptype stores.PathType, am stores.AcquireMode) func(context.Context, Worker) error {
	return func(ctx context.Context, worker Worker) error {
		return worker.Fetch(ctx, sector, ft, ptype, am)
	}
}

func (m *Manager) tryReadUnsealedPiece(ctx context.Context, sink io.Writer, sector abi.SectorID, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (foundUnsealed bool, readOk bool, selector WorkerSelector, returnErr error) {

	// acquire a lock purely for reading unsealed sectors
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTUnsealed, stores.FTNone); err != nil {
		returnErr = xerrors.Errorf("acquiring read sector lock: %w", err)
		return
	}

	// passing 0 spt because we only need it when allowFetch is true
	best, err := m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, 0, false)
	if err != nil {
		returnErr = xerrors.Errorf("read piece: checking for already existing unsealed sector: %w", err)
		return
	}

	foundUnsealed = len(best) > 0
	if foundUnsealed { // append to existing
		// There is unsealed sector, see if we can read from it

		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)

		err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
			readOk, err = w.ReadPiece(ctx, sink, sector, offset, size)
			return err
		})
		if err != nil {
			returnErr = xerrors.Errorf("reading piece from sealed sector: %w", err)
		}
	} else {
		selector = newAllocSelector(m.index, stores.FTUnsealed, stores.PathSealing)
	}
	return
}

func (m *Manager) ReadPiece(ctx context.Context, sink io.Writer, sector abi.SectorID, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid) error {
	foundUnsealed, readOk, selector, err := m.tryReadUnsealedPiece(ctx, sink, sector, offset, size)
	if err != nil {
		return err
	}
	if readOk {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed|stores.FTCache, stores.FTUnsealed); err != nil {
		return xerrors.Errorf("acquiring unseal sector lock: %w", err)
	}

	unsealFetch := func(ctx context.Context, worker Worker) error {
		if err := worker.Fetch(ctx, sector, stores.FTSealed|stores.FTCache, stores.PathSealing, stores.AcquireCopy); err != nil {
			return xerrors.Errorf("copy sealed/cache sector data: %v", err)
		}

		if foundUnsealed {
			if err := worker.Fetch(ctx, sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove); err != nil {
				return xerrors.Errorf("copy unsealed sector data: %v", err)
			}
		}
		return nil
	}

	if unsealed == cid.Undef {
		return xerrors.Errorf("cannot unseal piece (sector: %d, offset: %d size: %d) - unsealed cid is undefined", sector, offset, size)
	}
	err = m.sched.Schedule(ctx, sector, sealtasks.TTUnseal, selector, unsealFetch, func(ctx context.Context, w Worker) error {
		return w.UnsealPiece(ctx, sector, offset, size, ticket, unsealed)
	})
	if err != nil {
		return err
	}

	selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		readOk, err = w.ReadPiece(ctx, sink, sector, offset, size)
		return err
	})
	if err != nil {
		return xerrors.Errorf("reading piece from sealed sector: %v", err)
	}

	if !readOk {
		return xerrors.Errorf("failed to read unsealed piece")
	}

	return nil
}

func (m *Manager) NewSector(ctx context.Context, sector abi.SectorID) error {
	log.Warnf("stub NewSector")
	return nil
}

func sealingElapseStatistic(ctx context.Context, worker Worker, taskType sealtasks.TaskType, sector abi.SectorID, start int64, end int64, err error) {
	info, _ := worker.Info(ctx)
	address := info.Address
	if nil != err {
		log.Errorf("fail to run sector %v %v, elapsed %v s [%v, %v]: %v [%s]", sector.Number, taskType, end-start, start, end, err, address)
		return
	}
	log.Infof("success to run sector %v %v, elapsed %v s [%v, %v]: [%s]", sector.Number, taskType, end-start, start, end, address)
}

func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTUnsealed); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquiring sector lock: %v", err)
	}

	var selector WorkerSelector
	var err error
	if len(existingPieces) == 0 { // new
		selector = newAllocSelector(m.index, stores.FTUnsealed, stores.PathSealing)
	} else { // use existing
		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)
	}

	var out abi.PieceInfo
	err = m.sched.Schedule(ctx, sector, sealtasks.TTAddPiece, selector, schedNop, func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		info, ierr := w.Info(ctx)
		address := "unknow"
		if nil == ierr {
			address = info.Address
		}
		m.localStore.AddMayFailSector(ctx, sector, string(sealtasks.TTAddPiece), address)
		p, err := w.AddPiece(ctx, sector, existingPieces, sz, r)
		m.localStore.DropMayFailSector(ctx, sector)
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTAddPiece, sector, start, end, err)
		if err != nil {
			m.localStore.AddFailSector(ctx, sector, string(sealtasks.TTAddPiece), address)
			return err
		}
		out = p
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for sid, _ := range m.failSectors {
		if sid.Number == sector.Number {
			return nil, xerrors.Errorf("sector removed by miner fail")
		}
	}

	if err := m.index.StorageLock(ctx, sector, stores.FTUnsealed, stores.FTSealed|stores.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %v", err)
	}

	// TODO: also consider where the unsealed data sits

	selector := newAllocSelector(m.index, stores.FTCache|stores.FTSealed, stores.PathSealing)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		info, ierr := w.Info(ctx)
		address := "unknow"
		if nil == ierr {
			address = info.Address
		}
		m.localStore.AddMayFailSector(ctx, sector, string(sealtasks.TTPreCommit1), address)
		p, err := w.SealPreCommit1(ctx, sector, ticket, pieces)
		m.localStore.DropMayFailSector(ctx, sector)
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTPreCommit1, sector, start, end, err)
		if err != nil {
			info, ierr := w.Info(ctx)
			address := "unknow"
			if nil == ierr {
				address = info.Address
			}
			m.localStore.AddFailSector(ctx, sector, string(sealtasks.TTPreCommit1), address)
			return err
		}
		out = p
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for sid, _ := range m.failSectors {
		if sid.Number == sector.Number {
			return storage.SectorCids{}, xerrors.Errorf("sector removed by miner fail")
		}
	}

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %v", err)
	}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, true)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		info, ierr := w.Info(ctx)
		address := "unknow"
		if nil == ierr {
			address = info.Address
		}
		m.localStore.AddMayFailSector(ctx, sector, string(sealtasks.TTPreCommit2), address)
		p, err := w.SealPreCommit2(ctx, sector, phase1Out)
		m.localStore.DropMayFailSector(ctx, sector)
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTPreCommit2, sector, start, end, err)
		if err != nil {
			info, ierr := w.Info(ctx)
			address := "unknow"
			if nil == ierr {
				address = info.Address
			}
			m.localStore.AddFailSector(ctx, sector, string(sealtasks.TTPreCommit2), address)
			return err
		}
		out = p
		return nil
	})
	return out, err
}

func (m *Manager) MovingCache(ctx context.Context, sector abi.SectorID) error {
	log.Infof("try to move sector %d's cache to 2layer cache", sector)
	if err := m.storage.MoveCache(ctx, sector, stores.FTCache, true); err != nil {
		return xerrors.Errorf("manager moving sector %d cache to hdd error: %v", sector, err)
	}
	log.Infof("success to move sector %d's cache to 2layer cache", sector)
	return nil
}

func (m *Manager) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("acquiring sector lock: %v", err)
	}

	// NOTE: We set allowFetch to false in so that we always execute on a worker
	// with direct access to the data. We want to do that because this step is
	// generally very cheap / fast, and transferring data is not worth the effort
	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit1, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		p, err := w.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTCommit1, sector, start, end, err)
		if err != nil {
			return err
		}
		out = p
		return nil
	})
	return out, err
}

func (m *Manager) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		p, err := w.SealCommit2(ctx, sector, phase1Out)
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTCommit2, sector, start, end, err)
		if err != nil {
			return err
		}
		out = p
		return nil
	})

	return out, err
}

func (m *Manager) FinalizeSector(ctx context.Context, sector abi.SectorID, keepUnsealed []storage.Range) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTSealed|stores.FTUnsealed|stores.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %v", err)
	}

	unsealed := stores.FTUnsealed
	{
		unsealedStores, err := m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, 0, false)
		if err != nil {
			return xerrors.Errorf("finding unsealed sector: %v", err)
		}

		if len(unsealedStores) == 0 { // Is some edge-cases unsealed sector may not exist already, that's fine
			unsealed = stores.FTNone
		}
	}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	err := m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
		schedFetch(sector, stores.FTCache|stores.FTSealed|unsealed, stores.PathSealing, stores.AcquireMove),
		func(ctx context.Context, w Worker) error {
			start := time.Now().Unix()
			werr := w.FinalizeSector(ctx, sector, keepUnsealed)
			end := time.Now().Unix()
			sealingElapseStatistic(ctx, w, sealtasks.TTFinalize, sector, start, end, werr)
			return werr
		})
	if err != nil {
		return err
	}

	fetchSel := newAllocSelector(m.index, stores.FTCache|stores.FTSealed, stores.PathStorage)
	moveUnsealed := unsealed
	{
		if len(keepUnsealed) == 0 {
			moveUnsealed = stores.FTNone
		}
	}

	err = m.sched.Schedule(ctx, sector, sealtasks.TTFetch, fetchSel,
		schedFetch(sector, stores.FTCache|stores.FTSealed|moveUnsealed, stores.PathStorage, stores.AcquireMove),
		func(ctx context.Context, w Worker) error {
			start := time.Now().Unix()
			werr := w.MoveStorage(ctx, sector, stores.FTCache|stores.FTSealed|moveUnsealed)
			end := time.Now().Unix()
			sealingElapseStatistic(ctx, w, sealtasks.TTFetch, sector, start, end, werr)
			return werr
		})
	if err != nil {
		return xerrors.Errorf("moving sector to storage: %v", err)
	}

	return nil
}

func (m *Manager) ReleaseUnsealed(ctx context.Context, sector abi.SectorID, safeToFree []storage.Range) error {
	log.Warnw("ReleaseUnsealed todo")
	return nil
}

func (m *Manager) Remove(ctx context.Context, sector abi.SectorID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTSealed|stores.FTUnsealed|stores.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %v", err)
	}

	var err error

	if rerr := m.storage.Remove(ctx, sector, stores.FTSealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := m.storage.Remove(ctx, sector, stores.FTCache, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := m.storage.Remove(ctx, sector, stores.FTUnsealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	return err
}

func (m *Manager) StorageLocal(ctx context.Context) (map[stores.ID]string, error) {
	l, err := m.localStore.Local(ctx)
	if err != nil {
		return nil, err
	}

	out := map[stores.ID]string{}
	for _, st := range l {
		out[st.ID] = st.LocalPath
	}

	return out, nil
}

func (m *Manager) FsStat(ctx context.Context, id stores.ID) (fsutil.FsStat, error) {
	return m.storage.FsStat(ctx, id)
}

func (m *Manager) SchedDiag(ctx context.Context) (interface{}, error) {
	return m.sched.Info(ctx)
}

func (m *Manager) Close(ctx context.Context) error {
	return m.sched.Close(ctx)
}

var _ SectorManager = &Manager{}
