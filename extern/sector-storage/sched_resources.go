package sectorstorage

import (
	"sync"

	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func (a *activeResources) withResources(id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	for !a.canHandleRequest(r, id, "withResources", wr) {
		if a.cond == nil {
			a.cond = sync.NewCond(locker)
		}
		a.cond.Wait()
	}

	a.add(wr, r)

	err := cb()

	a.free(wr, r)
	if a.cond != nil {
		a.cond.Broadcast()
	}

	return err
}

func (a *activeResources) add(wr storiface.WorkerResources, r Resources) {
	a.gpuUsed = r.CanGPU
	if r.MultiThread() {
		a.cpuUse += wr.CPUs
	} else {
		a.cpuUse += uint64(r.Threads)
	}

	a.memUsedMin += r.MinMemory
	a.memUsedMax += r.MaxMemory

	a.taskUsed += 1
}

func (a *activeResources) free(wr storiface.WorkerResources, r Resources) {
	if r.CanGPU {
		a.gpuUsed = false
	}
	if r.MultiThread() {
		a.cpuUse -= wr.CPUs
	} else {
		a.cpuUse -= uint64(r.Threads)
	}

	a.memUsedMin -= r.MinMemory
	a.memUsedMax -= r.MaxMemory

	a.taskUsed -= 1
}

func (a *activeResources) canHandleRequest(needRes Resources, wid WorkerID, caller string, res storiface.WorkerResources) bool {

	// TODO: dedupe needRes.BaseMinMemory per task type (don't add if that task is already running)
	minNeedMem := res.MemReserved + a.memUsedMin + needRes.MinMemory + needRes.BaseMinMemory
	if minNeedMem > res.MemPhysical {
		log.Infof("sched: not scheduling on worker %d for %s; not enough physical memory - need: %dM, have %dM", wid, caller, minNeedMem/mib, res.MemPhysical/mib)
		return false
	}

	maxNeedMem := res.MemReserved + a.memUsedMax + needRes.MaxMemory + needRes.BaseMinMemory

	if maxNeedMem > res.MemSwap+res.MemPhysical {
		log.Infof("sched: not scheduling on worker %d for %s; not enough virtual memory - need: %dM, have %dM", wid, caller, maxNeedMem/mib, (res.MemSwap+res.MemPhysical)/mib)
		return false
	}

	if needRes.MultiThread() {
		if a.cpuUse > 0 {
			log.Infof("sched: not scheduling on worker %d for %s; multicore process needs %d threads, %d in use, target %d", wid, caller, res.CPUs, a.cpuUse, res.CPUs)
			return false
		}
	} else {
		if a.cpuUse+uint64(needRes.Threads)+uint64(needRes.Reserved) > res.CPUs {
			log.Infof("sched: not scheduling on worker %d for %s; not enough threads, need %d, %d in use, %d in reserved, target %d", wid, caller, needRes.Threads, a.cpuUse, needRes.Reserved, res.CPUs)
			return false
		}
	}

	if len(res.GPUs) > 0 && needRes.CanGPU {
		if a.gpuUsed {
			log.Infof("sched: not scheduling on worker %d for %s; GPU in use", wid, caller)
			return false
		}
	}

	//log.Infof("sched: to scheduling on worker %d for %s; up to task limit, %d in use, limit %d", wid, caller, a.taskUsed, needRes.TaskLimit)
	if needRes.TaskLimit > 0 && (a.taskUsed >= needRes.TaskLimit) {
		log.Infof("sched: not scheduling on worker %d for %s; up to task limit, %d in use, limit %d", wid, caller, a.taskUsed, needRes.TaskLimit)
		return false
	}

	return true
}

func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
	var max float64

	cpu := float64(a.cpuUse) / float64(wr.CPUs)
	max = cpu

	memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
	if memMin > max {
		max = memMin
	}

	memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
	if memMax > max {
		max = memMax
	}

	return max
}

func (wh *workerHandle) utilization() float64 {
	wh.wndLk.Lock()
	wh.lk.Lock()
	u := wh.active.utilization(wh.info.Resources)
	u += wh.preparing.utilization(wh.info.Resources)
	wh.lk.Unlock()
	for _, window := range wh.activeWindows {
		u += window.allocated.utilization(wh.info.Resources)
	}
	wh.wndLk.Unlock()

	return u
}
