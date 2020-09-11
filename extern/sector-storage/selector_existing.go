package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

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
		log.Infof("tropy: cannot get support worker task type: %v", err)
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		log.Infof("tropy: task %v is not supported in %v", task, tasks)
		return false, nil
	}

	paths, err := whnd.w.Paths(ctx)
	if err != nil {
		log.Infof("tropy: cannot get worker paths: %v", err)
		return false, xerrors.Errorf("getting worker paths: %w", err)
	}

	have := map[stores.ID]struct{}{}
	for _, path := range paths {
		log.Infof("tropy: find worker path [%v]: %v", path.ID, path.LocalPath)
		have[path.ID] = struct{}{}
	}

	best, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, spt, s.allowFetch)
	if err != nil {
		log.Infof("tropy: cannot find best storage path: %v", err)
		return false, xerrors.Errorf("finding best storage: %w", err)
	}

	for _, info := range best {
		log.Infof("tropy: best candidate path: %v / %v", info.ID, info.URLs)
		if _, ok := have[info.ID]; ok {
			log.Infof("tropy: best selected path for: %v worker [%v]", info.ID, whnd.info.Address)
			return true, nil
		}
	}

	log.Infof("tropy: cannot find best path for task %v worker [%v]", task, whnd.info.Address)
	return false, nil
}

func (s *existingSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &existingSelector{}
