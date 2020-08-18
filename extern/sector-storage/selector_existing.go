package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

type existingSelector struct {
	index      stores.SectorIndex
	sector     abi.SectorID
	alloc      stores.SectorFileType
	allowFetch bool
}

func newExistingSelector(index stores.SectorIndex, sector abi.SectorID, alloc stores.SectorFileType, allowFetch bool) *existingSelector {
	return &existingSelector{
		index:      index,
		sector:     sector,
		alloc:      alloc,
		allowFetch: allowFetch,
	}
}

func (s *existingSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.w.TaskTypes(ctx)
	if err != nil {
		log.Warnf("cannot get support worker task type: %v", err)
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		log.Warnf("task %v is not supported in %v", task, tasks)
		return false, nil
	}

	paths, err := whnd.w.Paths(ctx)
	if err != nil {
		log.Warnf("cannot get worker paths: %v", err)
		return false, xerrors.Errorf("getting worker paths: %w", err)
	}
	log.Warnf("find worker paths: %v", paths)

	have := map[stores.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	best, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, spt, s.allowFetch)
	if err != nil {
		return false, xerrors.Errorf("finding best storage: %w", err)
	}

	for _, info := range best {
		log.Warnf("best candidate path: %v", info)
		if _, ok := have[info.ID]; ok {
			return true, nil
		}
	}

	log.Warnf("cannot find best path for task %v", task)
	return false, nil
}

func (s *existingSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.active.utilization(a.info.Resources) < b.active.utilization(b.info.Resources), nil
}

var _ WorkerSelector = &existingSelector{}
