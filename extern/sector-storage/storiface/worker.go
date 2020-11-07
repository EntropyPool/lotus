package storiface

import (
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

type WorkerInfo struct {
	Hostname     string
	GroupName    string
	Address      string
	BigCache     bool
	SupportTasks []sealtasks.TaskType
	Resources    WorkerResources
}

type WorkerResources struct {
	MemPhysical uint64
	MemSwap     uint64

	MemReserved uint64 // Used by system / other processes

	CPUs uint64 // Logical cores
	GPUs []string
}

type TasksInfo struct {
	Running       int
	Prepared      int
	Waiting       int
	MaxConcurrent int
}

type WorkerStats struct {
	Info WorkerInfo

	MemUsedMin uint64
	MemUsedMax uint64
	GpuUsed    bool   // nolint
	CpuUse     uint64 // nolint
	Tasks      map[sealtasks.TaskType]TasksInfo
}

type WorkerJob struct {
	ID     uint64
	Sector abi.SectorID
	Task   sealtasks.TaskType

	RunWait int // 0 - running, 1+ - assigned
	Start   time.Time
}
