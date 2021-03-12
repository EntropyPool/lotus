package sectorstorage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
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
	storiface.WorkerCalls

	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)

	// Returns paths accessible to the worker
	Paths(context.Context) ([]stores.StoragePath, error)

	Info(context.Context) (storiface.WorkerInfo, error)

	Session(context.Context) (uuid.UUID, error)

	Close() error // TODO: do we need this?
}

type SectorManager interface {
	ReadPiece(context.Context, io.Writer, storage.SectorRef, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error
	PledgedJobs(ctx context.Context) int

	ffiwrapper.StorageSealer
	storage.Prover
	storiface.WorkerReturn
	FaultTracker
}

type WorkerID uuid.UUID // worker session UUID
var ClosedWorkerID = uuid.UUID{}

func (w WorkerID) String() string {
	return uuid.UUID(w).String()
}

type Manager struct {
	ls         stores.LocalStorage
	storage    *stores.Remote
	localStore *stores.Local
	remoteHnd  *stores.FetchHandler
	index      stores.SectorIndex

	sched *scheduler

	storage.Prover

	workLk sync.Mutex
	work   *statestore.StateStore

	callToWork map[storiface.CallID]WorkID
	// used when we get an early return and there's no callToWork mapping
	callRes map[storiface.CallID]chan result

	results map[WorkID]result
	waitRes map[WorkID]chan struct{}
}

type result struct {
	r   interface{}
	err error
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

type WorkerStateStore *statestore.StateStore
type ManagerStateStore *statestore.StateStore

func New(ctx context.Context, ls stores.LocalStorage, si stores.SectorIndex, sc SealerConfig, urls URLs, sa StorageAuth, wss WorkerStateStore, mss ManagerStateStore) (*Manager, error) {
	lstor, err := stores.NewLocal(ctx, ls, si, urls)
	if err != nil {
		return nil, err
	}

	prover, err := ffiwrapper.New(&readonlyProvider{stor: lstor, index: si})
	if err != nil {
		return nil, xerrors.Errorf("creating prover instance: %v", err)
	}

	stor := stores.NewRemote(lstor, si, http.Header(sa), sc.ParallelFetchLimit)

	m := &Manager{
		ls:         ls,
		storage:    stor,
		localStore: lstor,
		remoteHnd:  &stores.FetchHandler{Local: lstor},
		index:      si,

		sched: newScheduler(),

		work:       mss,
		callToWork: map[storiface.CallID]WorkID{},
		callRes:    map[storiface.CallID]chan result{},
		results:    map[WorkID]result{},
		waitRes:    map[WorkID]chan struct{}{},
		Prover:     prover,
	}

	m.setupWorkTracker()

	m.sched.SetStorage(&EStorage{
		ctx:   ctx,
		index: si,
		ls:    m.localStore,
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
		TaskTypes: localTasks,
	}, stor, lstor, si, m, wss))
	if err != nil {
		return nil, xerrors.Errorf("adding local worker: %v", err)
	}

	return m, nil
}

func (m *Manager) UpdateLocalStorage(ctx context.Context, path string) error {
	path, err := homedir.Expand(path)
	if err != nil {
		return xerrors.Errorf("expanding local path: %w", err)
	}

	if err := m.localStore.UpdatePath(ctx, path); err != nil {
		return xerrors.Errorf("opening local path: %w", err)
	}

	return nil
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
	return m.sched.runWorker(ctx, w)
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.remoteHnd.ServeHTTP(w, r)
}

func schedNop(context.Context, Worker) error {
	return nil
}

func (m *Manager) schedFetch(sector storage.SectorRef, ft storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) func(context.Context, Worker) error {
	return func(ctx context.Context, worker Worker) error {
		_, err := m.waitSimpleCall(ctx)(worker.Fetch(ctx, sector, ft, ptype, am))
		return err
	}
}

func (m *Manager) readPiece(sink io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, rok *bool) func(ctx context.Context, w Worker) error {
	return func(ctx context.Context, w Worker) error {
		r, err := m.waitSimpleCall(ctx)(w.ReadPiece(ctx, sink, sector, offset, size))
		if err != nil {
			return err
		}
		if r != nil {
			*rok = r.(bool)
		}
		return nil
	}
}

func (m *Manager) tryReadUnsealedPiece(ctx context.Context, sink io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (foundUnsealed bool, readOk bool, selector WorkerSelector, returnErr error) {

	// acquire a lock purely for reading unsealed sectors
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTUnsealed, storiface.FTNone); err != nil {
		returnErr = xerrors.Errorf("acquiring read sector lock: %w", err)
		return
	}

	// passing 0 spt because we only need it when allowFetch is true
	best, err := m.index.StorageFindSector(ctx, sector.ID, storiface.FTUnsealed, 0, false)
	if err != nil {
		returnErr = xerrors.Errorf("read piece: checking for already existing unsealed sector: %w", err)
		return
	}

	foundUnsealed = len(best) > 0
	if foundUnsealed { // append to existing
		// There is unsealed sector, see if we can read from it

		selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)

		err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, m.schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove),
			m.readPiece(sink, sector, offset, size, &readOk))
		if err != nil {
			returnErr = xerrors.Errorf("reading piece from sealed sector: %w", err)
		}
	} else {
		selector = newAllocSelector(m.index, storiface.FTUnsealed, storiface.PathSealing)
	}
	return
}

func (m *Manager) ReadPiece(ctx context.Context, sink io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid) error {
	foundUnsealed, readOk, selector, err := m.tryReadUnsealedPiece(ctx, sink, sector, offset, size)
	if err != nil {
		return err
	}
	if readOk {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed|storiface.FTCache, storiface.FTUnsealed); err != nil {
		return xerrors.Errorf("acquiring unseal sector lock: %w", err)
	}

	unsealFetch := func(ctx context.Context, worker Worker) error {
		if _, err := m.waitSimpleCall(ctx)(worker.Fetch(ctx, sector, storiface.FTSealed|storiface.FTCache, storiface.PathSealing, storiface.AcquireCopy)); err != nil {
			return xerrors.Errorf("copy sealed/cache sector data: %w", err)
		}

		if foundUnsealed {
			if _, err := m.waitSimpleCall(ctx)(worker.Fetch(ctx, sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove)); err != nil {
				return xerrors.Errorf("copy unsealed sector data: %w", err)
			}
		}
		return nil
	}

	if unsealed == cid.Undef {
		return xerrors.Errorf("cannot unseal piece (sector: %d, offset: %d size: %d) - unsealed cid is undefined", sector, offset, size)
	}

	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return xerrors.Errorf("getting sector size: %w", err)
	}

	err = m.sched.Schedule(ctx, sector, sealtasks.TTUnseal, selector, unsealFetch, func(ctx context.Context, w Worker) error {
		// TODO: make restartable

		// NOTE: we're unsealing the whole sector here as with SDR we can't really
		//  unseal the sector partially. Requesting the whole sector here can
		//  save us some work in case another piece is requested from here
		_, err := m.waitSimpleCall(ctx)(w.UnsealPiece(ctx, sector, 0, abi.PaddedPieceSize(ssize).Unpadded(), ticket, unsealed))
		return err
	})
	if err != nil {
		return err
	}

	selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, m.schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove),
		m.readPiece(sink, sector, offset, size, &readOk))
	if err != nil {
		return xerrors.Errorf("reading piece from sealed sector: %v", err)
	}

	if !readOk {
		return xerrors.Errorf("failed to read unsealed piece")
	}

	return nil
}

func (m *Manager) NewSector(ctx context.Context, sector storage.SectorRef) error {
	log.Warnf("stub NewSector")
	return nil
}

const sealingStart = "start"
const sealingEnd = "end"

func sealingElapseStatistic(ctx context.Context, worker Worker, taskType sealtasks.TaskType, sector storage.SectorRef, stage string, start int64, end int64, err error) {
	info, _ := worker.Info(ctx)
	address := info.Address

	switch stage {
	case sealingStart:
		log.Infow("run task start",
			"worker", address,
			"taskType", taskType,
			"sectorNumber", sector.ID.Number,
			"start", start,
		)
	case sealingEnd:
		if nil != err {
			log.Errorw("run task end",
				"worker", address,
				"taskType", taskType,
				"sectorNumber", sector.ID.Number,
				"start", end,
				"elapsed", end-start,
				"error", err,
			)
			return
		}
		log.Infow("run task end",
			"worker", address,
			"taskType", taskType,
			"sectorNumber", sector.ID.Number,
			"start", end,
			"elapsed", end-start,
		)
	}
}

func (m *Manager) AddPiece(ctx context.Context, sector storage.SectorRef, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTNone, storiface.FTUnsealed); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquiring sector lock: %v", err)
	}

	var selector WorkerSelector
	var err error
	if len(existingPieces) == 0 { // new
		selector = newAllocSelector(m.index, storiface.FTUnsealed, storiface.PathSealing)
	} else { // use existing
		selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)
	}

	var out abi.PieceInfo
	err = m.sched.Schedule(ctx, sector, sealtasks.TTAddPiece, selector, schedNop, func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTAddPiece, sector, sealingStart, start, 0, nil)
		p, err := m.waitSimpleCall(ctx)(w.AddPiece(ctx, sector, existingPieces, sz, r))
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTAddPiece, sector, sealingEnd, start, end, err)

		if err == nil && p != nil {
			out = p.(abi.PieceInfo)
		}

		return err
	})

	return out, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTUnsealed, storiface.FTSealed|storiface.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// TODO: also consider where the unsealed data sits

	selector := newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)
	// selector := newAllocSelector(m.index, storiface.FTCache|storiface.FTSealed, storiface.PathSealing)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, m.schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTPreCommit1, sector, sealingStart, start, 0, nil)
		p, err := m.waitSimpleCall(ctx)(w.SealPreCommit1(ctx, sector, ticket, pieces))
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTPreCommit1, sector, sealingEnd, start, end, err)

		if err == nil && p != nil {
			out = p.(storage.PreCommit1Out)
		}

		return err
	})

	return out, err
}

func (m *Manager) MovingCache(ctx context.Context, sector storage.SectorRef) error {
	log.Infof("try to move sector %v's cache to 2layer cache", sector)
	if err := m.storage.MoveCache(ctx, sector, storiface.FTCache, true); err != nil {
		return xerrors.Errorf("manager moving sector %v cache to hdd error: %v", sector, err)
	}
	m.sched.MoveCacheDone(sector)
	log.Infof("success to move sector %v's cache to 2layer cache", sector)
	return nil
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed, storiface.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %v", err)
	}

	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, m.schedFetch(sector, storiface.FTCache|storiface.FTSealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTPreCommit2, sector, sealingStart, start, 0, nil)
		p, err := m.waitSimpleCall(ctx)(w.SealPreCommit2(ctx, sector, phase1Out))
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTPreCommit2, sector, sealingEnd, start, end, err)

		if err == nil && p != nil {
			out = p.(storage.SectorCids)
		}

		return err
	})

	if err != nil {
		return storage.SectorCids{}, err
	}

	return out, err
}

func (m *Manager) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed, storiface.FTCache); err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// NOTE: We set allowFetch to false in so that we always execute on a worker
	// with direct access to the data. We want to do that because this step is
	// generally very cheap / fast, and transferring data is not worth the effort
	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit1, selector, m.schedFetch(sector, storiface.FTCache|storiface.FTSealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTCommit1, sector, sealingStart, start, 0, nil)
		p, err := m.waitSimpleCall(ctx)(w.SealCommit1(ctx, sector, ticket, seed, pieces, cids))
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTCommit1, sector, sealingEnd, start, end, err)

		if err == nil && p != nil {
			out = p.(storage.Commit1Out)
		}

		return err
	})

	if err != nil {
		return nil, err
	}

	return out, err
}

func (m *Manager) SealCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTCommit2, sector, phase1Out)
	if err != nil {
		return storage.Proof{}, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	waitRes := func() {
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.Proof)
		}
	}

	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		start := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTCommit2, sector, sealingStart, start, 0, nil)
		p, err := m.waitSimpleCall(ctx)(w.SealCommit2(ctx, sector, phase1Out))
		end := time.Now().Unix()
		sealingElapseStatistic(ctx, w, sealtasks.TTCommit2, sector, sealingEnd, start, end, err)

		if err == nil && p != nil {
			out = p.(storage.Proof)
		}

		return err
	})

	if err != nil {
		return nil, err
	}

	return out, err
}

func (m *Manager) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTNone, storiface.FTSealed|storiface.FTUnsealed|storiface.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	unsealed := storiface.FTUnsealed
	{
		unsealedStores, err := m.index.StorageFindSector(ctx, sector.ID, storiface.FTUnsealed, 0, false)
		if err != nil {
			return xerrors.Errorf("finding unsealed sector: %v", err)
		}

		if len(unsealedStores) == 0 { // Is some edge-cases unsealed sector may not exist already, that's fine
			unsealed = storiface.FTNone
		}
	}

	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, false)

	err := m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
		m.schedFetch(sector, storiface.FTCache|storiface.FTSealed|unsealed, storiface.PathSealing, storiface.AcquireMove),
		func(ctx context.Context, w Worker) error {
			start := time.Now().Unix()
			sealingElapseStatistic(ctx, w, sealtasks.TTFinalize, sector, sealingStart, start, 0, nil)
			_, werr := m.waitSimpleCall(ctx)(w.FinalizeSector(ctx, sector, keepUnsealed))
			end := time.Now().Unix()
			sealingElapseStatistic(ctx, w, sealtasks.TTFinalize, sector, sealingEnd, start, end, werr)
			return werr
		})
	if err != nil {
		return err
	}

	fetchSel := newAllocSelector(m.index, storiface.FTCache|storiface.FTSealed, storiface.PathStorage)
	moveUnsealed := unsealed
	{
		if len(keepUnsealed) == 0 {
			moveUnsealed = storiface.FTNone
		}
	}

	err = m.sched.Schedule(ctx, sector, sealtasks.TTFetch, fetchSel,
		m.schedFetch(sector, storiface.FTCache|storiface.FTSealed|moveUnsealed, storiface.PathStorage, storiface.AcquireMove),
		func(ctx context.Context, w Worker) error {
			start := time.Now().Unix()
			sealingElapseStatistic(ctx, w, sealtasks.TTFetch, sector, sealingStart, start, 0, nil)
			_, werr := m.waitSimpleCall(ctx)(w.MoveStorage(ctx, sector, storiface.FTCache|storiface.FTSealed|moveUnsealed))
			end := time.Now().Unix()
			sealingElapseStatistic(ctx, w, sealtasks.TTFetch, sector, sealingEnd, start, end, werr)
			return werr
		})
	if err != nil {
		return xerrors.Errorf("moving sector to storage: %v", err)
	}

	return nil
}

func (m *Manager) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) error {
	log.Warnw("ReleaseUnsealed todo")
	return nil
}

func (m *Manager) Remove(ctx context.Context, sector storage.SectorRef) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTNone, storiface.FTSealed|storiface.FTUnsealed|storiface.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	var err error

	if rerr := m.storage.Remove(ctx, sector.ID, storiface.FTSealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := m.storage.Remove(ctx, sector.ID, storiface.FTCache, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := m.storage.Remove(ctx, sector.ID, storiface.FTUnsealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	m.sched.esched.RemoveSector(sector)

	return err
}

func (m *Manager) ReturnAddPiece(ctx context.Context, callID storiface.CallID, pi abi.PieceInfo, err *storiface.CallError) error {
	return m.returnResult(callID, pi, err)
}

func (m *Manager) ReturnSealPreCommit1(ctx context.Context, callID storiface.CallID, p1o storage.PreCommit1Out, err *storiface.CallError) error {
	return m.returnResult(callID, p1o, err)
}

func (m *Manager) ReturnSealPreCommit2(ctx context.Context, callID storiface.CallID, sealed storage.SectorCids, err *storiface.CallError) error {
	return m.returnResult(callID, sealed, err)
}

func (m *Manager) ReturnSealCommit1(ctx context.Context, callID storiface.CallID, out storage.Commit1Out, err *storiface.CallError) error {
	return m.returnResult(callID, out, err)
}

func (m *Manager) ReturnSealCommit2(ctx context.Context, callID storiface.CallID, proof storage.Proof, err *storiface.CallError) error {
	return m.returnResult(callID, proof, err)
}

func (m *Manager) ReturnFinalizeSector(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) ReturnReleaseUnsealed(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) ReturnMoveStorage(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) ReturnUnsealPiece(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) ReturnReadPiece(ctx context.Context, callID storiface.CallID, ok bool, err *storiface.CallError) error {
	return m.returnResult(callID, ok, err)
}

func (m *Manager) ReturnFetch(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
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

func (m *Manager) SchedDiag(ctx context.Context, doSched bool) (interface{}, error) {
	if doSched {
		select {
		case m.sched.workerChange <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	si, err := m.sched.Info(ctx)
	if err != nil {
		return nil, err
	}

	type SchedInfo interface{}
	i := struct {
		SchedInfo

		ReturnedWork []string
		Waiting      []string

		CallToWork map[string]string

		EarlyRet []string
	}{
		SchedInfo: si,

		CallToWork: map[string]string{},
	}

	m.workLk.Lock()

	for w := range m.results {
		i.ReturnedWork = append(i.ReturnedWork, w.String())
	}

	for id := range m.callRes {
		i.EarlyRet = append(i.EarlyRet, id.String())
	}

	for w := range m.waitRes {
		i.Waiting = append(i.Waiting, w.String())
	}

	for c, w := range m.callToWork {
		i.CallToWork[c.String()] = w.String()
	}

	m.workLk.Unlock()

	return i, nil
}

func (m *Manager) Close(ctx context.Context) error {
	return m.sched.Close(ctx)
}

func (m *Manager) PledgedJobs(ctx context.Context) int {
	return m.sched.esched.PledgedJobs()
}

var _ SectorManager = &Manager{}
