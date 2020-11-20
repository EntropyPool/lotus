package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type existingSelector struct {
	index      stores.SectorIndex
	sector     abi.SectorID
	alloc      storiface.SectorFileType
	allowFetch bool
}

func newExistingSelector(index stores.SectorIndex, sector abi.SectorID, alloc storiface.SectorFileType, allowFetch bool) *existingSelector {
	return &existingSelector{
		index:      index,
		sector:     sector,
		alloc:      alloc,
		allowFetch: allowFetch,
	}
}

func (s *existingSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, wi interface{}) (bool, error) {
	var w Worker
	switch wi.(type) {
	case *workerHandle:
		w = wi.(*workerHandle).workerRpc
	case *eWorkerHandle:
		w = wi.(*eWorkerHandle).w
	}
	tasks, err := w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		return false, nil
	}

	paths, err := w.Paths(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting worker paths: %w", err)
	}

	have := map[stores.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	// Always use the one which already have the sector
	var best []stores.SectorStorageInfo = make([]stores.SectorStorageInfo, 0)

	ssize, err := spt.SectorSize()
	if err != nil {
		return false, xerrors.Errorf("getting sector size: %w", err)
	}
	bestExist, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, ssize, false)
	if err == nil {
		best = append(best, bestExist...)
	}

	bestFetch, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, ssize, s.allowFetch)
	if err == nil {
		best = append(best, bestFetch...)
	}

	if 0 == len(best) {
		return false, xerrors.Errorf("find best storage: %w", err)
	}

	for _, info := range best {
		if _, ok := have[info.ID]; ok {
			return true, nil
		}
	}

	return false, nil
}

func (s *existingSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &existingSelector{}
