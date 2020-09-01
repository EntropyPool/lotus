package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

type allocSelector struct {
	index stores.SectorIndex
	alloc stores.SectorFileType
	ptype stores.PathType
}

func newAllocSelector(index stores.SectorIndex, alloc stores.SectorFileType, ptype stores.PathType) *allocSelector {
	return &allocSelector{
		index: index,
		alloc: alloc,
		ptype: ptype,
	}
}

func (s *allocSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.w.TaskTypes(ctx)
	if err != nil {
		log.Infof("tropy: cannot get supported worker task type: [%v]", err)
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		log.Infof("tropy: %v is not supported in [%v]", task, tasks)
		return false, nil
	}

	paths, err := whnd.w.Paths(ctx)
	if err != nil {
		log.Infof("tropy: cannot get paths from worker: [%v]", err)
		return false, xerrors.Errorf("getting worker paths: %w", err)
	}

	have := map[stores.ID]struct{}{}
	for _, path := range paths {
		log.Infof("tropy: find worker path [%v]: %v", path.ID, path.LocalPath)
		have[path.ID] = struct{}{}
	}

	best, err := s.index.StorageBestAlloc(ctx, s.alloc, spt, s.ptype)
	if err != nil {
		log.Infof("tropy: cannot find best alloc storage: [%v]", err)
		return false, xerrors.Errorf("finding best alloc storage: %w", err)
	}

	for _, info := range best {
		log.Infof("tropy: best candidate path: %v / %v", info.ID, info.URLs)
		if _, ok := have[info.ID]; ok {
			log.Infof("tropy: best selected path: %v / %v worker [%v]", info.ID, info.URLs, whnd.info.Address)
			return true, nil
		}
	}

	log.Infof("tropy: cannot alloc best path for task %v worker [%v]", task, whnd.info.Address)
	return false, nil
}

func (s *allocSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &allocSelector{}
