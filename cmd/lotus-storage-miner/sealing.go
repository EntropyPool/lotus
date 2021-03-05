package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
)

var sealingCmd = &cli.Command{
	Name:  "sealing",
	Usage: "interact with sealing pipeline",
	Subcommands: []*cli.Command{
		sealingJobsCmd,
		sealingWorkersCmd,
		sealingSchedDiagCmd,
		sealingAbortCmd,
		scheduleAbortCmd,
		scheduleEnableDebugCmd,
		sealingGasAdjustCmd,
		sealingSetWorkerModeCmd,
		sealingSetWorkerReservedSpaceCmd,
	},
}

var sealingWorkersCmd = &cli.Command{
	Name:  "workers",
	Usage: "list workers",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "color"},
	},
	Action: func(cctx *cli.Context) error {
		color.NoColor = !cctx.Bool("color")

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		stats, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return err
		}

		type sortableStat struct {
			id uuid.UUID
			storiface.WorkerStats
		}

		st := make([]sortableStat, 0, len(stats))
		for id, stat := range stats {
			st = append(st, sortableStat{id, stat})
		}

		sort.Slice(st, func(i, j int) bool {
			return st[i].id.String() < st[j].id.String()
		})

		for _, stat := range st {
			gpuUse := "not "
			gpuCol := color.FgBlue
			if stat.GpuUsed {
				gpuCol = color.FgGreen
				gpuUse = ""
			}

			var flags string = " ("
			if stat.Maintaining {
				flags += color.RedString("M")
			}
			if stat.RejectNewTask {
				flags += color.RedString("R")
			}
			flags += ")"

			addressStr := stat.Info.Address
			if 0 == len(addressStr) {
				addressStr = "localhost"
			}
			fmt.Printf("Worker %s (%s), host %s/%s%s\n", stat.id, stat.State,
				color.MagentaString(stat.Info.Hostname),
				color.MagentaString(addressStr), flags)

			taskTypes := ""
			sort.Slice(stat.Info.SupportTasks, func(i, j int) bool {
				return strings.Compare(string(stat.Info.SupportTasks[i]), string(stat.Info.SupportTasks[j])) < 0
			})

			fmt.Printf("\tGRP:  %s\n", color.MagentaString(stat.Info.GroupName))
			for _, taskType := range stat.Info.SupportTasks {
				taskTypes = fmt.Sprintf("%s\n\t      ", taskTypes)
				maxConcurrent := stat.Tasks[taskType].MaxConcurrent
				taskTypes = fmt.Sprintf("%s| %4s | %7d | %8d | %8d | %7d | %13d |",
					taskTypes, taskType.Short(),
					stat.Tasks[taskType].Running, stat.Tasks[taskType].Prepared,
					stat.Tasks[taskType].Cleaning, stat.Tasks[taskType].Waiting,
					maxConcurrent)
			}
			fmt.Printf("\t      ------------------------------------------------------------------\n")
			fmt.Printf("\tTSK:  | Type | Running | Prepared | Cleaning | Waiting | MaxConcurrent |%s\n", taskTypes)
			fmt.Printf("\t      ------------------------------------------------------------------\n")

			workerStores := ""
			for _, store := range stat.Stores {
				workerStores = fmt.Sprintf("%s\n\t      ", workerStores)
				workerStores = fmt.Sprintf("%s| %8s | %14v | %14v | %14v | %10v |",
					workerStores, strings.Split(store.ID, "-")[0],
					store.Total, store.Available,
					store.Reserved, store.MaxReached)
			}
			fmt.Printf("\t      ----------------------------------------------------------------------------\n")
			fmt.Printf("\tSTO:  |    ID    |      Total     |      Avail     |      Rsvd      | MaxReached |%s\n", workerStores)
			fmt.Printf("\t      ----------------------------------------------------------------------------\n")

			var barCols = uint64(64)
			cpuBars := int(stat.CpuUse * barCols / stat.Info.Resources.CPUs)
			cpuBar := strings.Repeat("|", cpuBars) + strings.Repeat(" ", int(barCols)-cpuBars)

			fmt.Printf("\tCPU:  [%s] %d/%d core(s) in use\n",
				color.GreenString(cpuBar), stat.CpuUse, stat.Info.Resources.CPUs)

			ramBarsRes := int(stat.Info.Resources.MemReserved * barCols / stat.Info.Resources.MemPhysical)
			ramBarsUsed := int(stat.MemUsedMin * barCols / stat.Info.Resources.MemPhysical)
			ramBar := color.YellowString(strings.Repeat("|", ramBarsRes)) +
				color.GreenString(strings.Repeat("|", ramBarsUsed)) +
				strings.Repeat(" ", int(barCols)-ramBarsUsed-ramBarsRes)

			vmem := stat.Info.Resources.MemPhysical + stat.Info.Resources.MemSwap

			vmemBarsRes := int(stat.Info.Resources.MemReserved * barCols / vmem)
			vmemBarsUsed := int(stat.MemUsedMax * barCols / vmem)
			vmemBar := color.YellowString(strings.Repeat("|", vmemBarsRes)) +
				color.GreenString(strings.Repeat("|", vmemBarsUsed)) +
				strings.Repeat(" ", int(barCols)-vmemBarsUsed-vmemBarsRes)

			fmt.Printf("\tRAM:  [%s] %d%% %s/%s\n", ramBar,
				(stat.Info.Resources.MemReserved+stat.MemUsedMin)*100/stat.Info.Resources.MemPhysical,
				types.SizeStr(types.NewInt(stat.Info.Resources.MemReserved+stat.MemUsedMin)),
				types.SizeStr(types.NewInt(stat.Info.Resources.MemPhysical)))

			fmt.Printf("\tVMEM: [%s] %d%% %s/%s\n", vmemBar,
				(stat.Info.Resources.MemReserved+stat.MemUsedMax)*100/vmem,
				types.SizeStr(types.NewInt(stat.Info.Resources.MemReserved+stat.MemUsedMax)),
				types.SizeStr(types.NewInt(vmem)))

			for _, gpu := range stat.Info.Resources.GPUs {
				fmt.Printf("\tGPU: %s\n", color.New(gpuCol).Sprintf("%s, %sused", gpu, gpuUse))
			}
		}

		return nil
	},
}

var sealingJobsCmd = &cli.Command{
	Name:  "jobs",
	Usage: "list running jobs",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "color"},
		&cli.BoolFlag{
			Name:  "show-ret-done",
			Usage: "show returned but not consumed calls",
		},
	},
	Action: func(cctx *cli.Context) error {
		color.NoColor = !cctx.Bool("color")

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		jobs, err := nodeApi.WorkerJobs(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker jobs: %w", err)
		}

		type line struct {
			storiface.WorkerJob
			wid uuid.UUID
		}

		lines := make([]line, 0)

		for wid, jobs := range jobs {
			for _, job := range jobs {
				lines = append(lines, line{
					WorkerJob: job,
					wid:       wid,
				})
			}
		}

		// oldest first
		sort.Slice(lines, func(i, j int) bool {
			if lines[i].RunWait != lines[j].RunWait {
				return lines[i].RunWait < lines[j].RunWait
			}
			if lines[i].Start.Equal(lines[j].Start) {
				return lines[i].ID.ID.String() < lines[j].ID.ID.String()
			}
			return lines[i].Start.Before(lines[j].Start)
		})

		workerHostnames := map[uuid.UUID]string{}

		wst, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker stats: %w", err)
		}

		for wid, st := range wst {
			workerHostnames[wid] = st.Info.Address
		}

		tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "ID\tSector\tWorker\tHostname\tTask\tState\tTime\n")

		for _, l := range lines {
			state := "running"
			switch {
			case l.RunWait > 0:
				state = fmt.Sprintf("assigned(%d)", l.RunWait-1)
			case l.RunWait == storiface.RWRetDone:
				if !cctx.Bool("show-ret-done") {
					continue
				}
				state = "ret-done"
			case l.RunWait == storiface.RWReturned:
				state = "returned"
			case l.RunWait == storiface.RWRetWait:
				state = "ret-wait"
			}
			dur := "n/a"
			if !l.Start.IsZero() {
				dur = time.Now().Sub(l.Start).Truncate(time.Millisecond * 100).String()
			}

			hostname, ok := workerHostnames[l.wid]
			if !ok {
				hostname = l.Hostname
			}

			_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
				hex.EncodeToString(l.ID.ID[:4]),
				l.Sector.Number,
				hex.EncodeToString(l.wid[:4]),
				hostname,
				l.Task.Short(),
				state,
				dur)
		}

		return tw.Flush()
	},
}

var sealingSchedDiagCmd = &cli.Command{
	Name:  "sched-diag",
	Usage: "Dump internal scheduler state",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name: "force-sched",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		st, err := nodeApi.SealingSchedDiag(ctx, cctx.Bool("force-sched"))
		if err != nil {
			return err
		}

		j, err := json.MarshalIndent(&st, "", "  ")
		if err != nil {
			return err
		}

		fmt.Println(string(j))

		return nil
	},
}

var sealingAbortCmd = &cli.Command{
	Name:      "abort",
	Usage:     "Abort a running job",
	ArgsUsage: "[callid]",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("expected 1 argument")
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		jobs, err := nodeApi.WorkerJobs(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker jobs: %w", err)
		}

		var job *storiface.WorkerJob
	outer:
		for _, workerJobs := range jobs {
			for _, j := range workerJobs {
				if strings.HasPrefix(j.ID.ID.String(), cctx.Args().First()) {
					j := j
					job = &j
					break outer
				}
			}
		}

		if job == nil {
			return xerrors.Errorf("job with specified id prefix not found")
		}

		fmt.Printf("aborting job %s, task %s, sector %d, running on host %s\n", job.ID.String(), job.Task.Short(), job.Sector.Number, job.Hostname)

		return nodeApi.SealingAbort(ctx, job.ID)
	},
}

var scheduleAbortCmd = &cli.Command{
	Name:      "sched-abort",
	Usage:     "Abort a schedule waiting job",
	ArgsUsage: "[sector]",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("expected 1 argument")
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		jobs, err := nodeApi.WorkerJobs(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker jobs: %w", err)
		}

		var job *storiface.WorkerJob
	outer:
		for _, workerJobs := range jobs {
			for _, j := range workerJobs {
				number, _ := strconv.ParseUint(cctx.Args().First(), 10, 64)
				if uint(j.Sector.Number) == uint(number) {
					j := j
					job = &j
					break outer
				}
			}
		}

		if job == nil {
			return xerrors.Errorf("job with specified id prefix not found")
		}

		fmt.Printf("aborting job %s, task %s, sector %d, running on host %s\n", job.ID.String(), job.Task.Short(), job.Sector.Number, job.Hostname)

		sector := storage.SectorRef{
			ID:        job.Sector,
			ProofType: 0,
		}

		return nodeApi.ScheduleAbort(ctx, sector)
	},
}

var sealingGasAdjustCmd = &cli.Command{
	Name:  "sealing-adjust",
	Usage: "Adjust sealing gas",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "prefer-sector-on-chain",
			Value: true,
		},
		&cli.Float64Flag{
			Name:  "max-pre-commit-gas-fee",
			Value: 0.07,
		},
		&cli.Float64Flag{
			Name:  "max-commit-gas-fee",
			Value: 0.3,
		},
		&cli.BoolFlag{
			Name:  "enable-auto-pledge",
			Value: true,
		},
		&cli.IntFlag{
			Name:  "auto-pledge-balance-threshold",
			Value: 300,
		},
		&cli.IntFlag{
			Name:  "sched-idle-cpus",
			Value: 0,
		},
		&cli.IntFlag{
			Name:  "sched-usable-cpus",
			Value: 0,
		},
		&cli.IntFlag{
			Name:  "sched-gpu-tasks",
			Value: 0,
		},
		&cli.IntFlag{
			Name:  "sched-concurrent-add-piece",
			Value: 0,
		},
	},
	Action: func(cctx *cli.Context) error {
		preferSectorOnChain := cctx.Bool("prefer-sector-on-chain")
		maxPrecommitGasFee := cctx.Float64("max-pre-commit-gas-fee")
		maxCommitGasFee := cctx.Float64("max-commit-gas-fee")
		autoPledgeBalanceThreshold := cctx.Int("auto-pledge-balance-threshold")
		enableAutoPledge := cctx.Bool("enable-auto-pledge")
		schedIdleCpus := cctx.Int("sched-idle-cpus")
		schedUsableCpus := cctx.Int("sched-usable-cpus")
		schedGpuTasks := cctx.Int("sched-gpu-tasks")
		schedConcurrentAddPiece := cctx.Int("sched-concurrent-add-piece")

		ctx := lcli.ReqContext(cctx)

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		err = nodeApi.SealingSetPreferSectorOnChain(ctx, preferSectorOnChain)
		if err != nil {
			return err
		}

		gasFee := abi.TokenAmount(types.MustParseFIL(fmt.Sprintf("%v FIL", maxPrecommitGasFee)))
		err = nodeApi.SetMaxPreCommitGasFee(ctx, gasFee)
		if err != nil {
			return err
		}

		gasFee = abi.TokenAmount(types.MustParseFIL(fmt.Sprintf("%v FIL", maxCommitGasFee)))
		err = nodeApi.SetMaxCommitGasFee(ctx, gasFee)
		if err != nil {
			return err
		}

		err = nodeApi.SealingSetEnableAutoPledge(ctx, enableAutoPledge)
		if err != nil {
			return err
		}

		balance := abi.TokenAmount(types.MustParseFIL(fmt.Sprintf("%v FIL", autoPledgeBalanceThreshold)))
		err = nodeApi.SealingSetAutoPledgeBalanceThreshold(ctx, balance)
		if err != nil {
			return err
		}

		err = nodeApi.SetScheduleConcurrent(ctx, schedIdleCpus, schedUsableCpus, schedConcurrentAddPiece)
		if err != nil {
			return err
		}

		err = nodeApi.SetScheduleGpuConcurrentTasks(ctx, schedGpuTasks)
		if err != nil {
			return err
		}

		fmt.Printf("Sealing Adjust ---\n")
		fmt.Printf("  PreCommit GAS:             %v FIL\n", maxPrecommitGasFee)
		fmt.Printf("  Commit GAS:                %v FIL\n", maxCommitGasFee)
		fmt.Printf("  Prefer Sector On Chain:    %v\n", preferSectorOnChain)
		fmt.Printf("  Enable Auto Pledge:        %v\n", enableAutoPledge)
		fmt.Printf("  Auto Pledge Threshold:     %v FIL\n", autoPledgeBalanceThreshold)
		fmt.Printf("  Sched CPUs:                I %v / U %v / AP %v\n", schedIdleCpus, schedUsableCpus, schedConcurrentAddPiece)
		fmt.Printf("  Sched GPU Tasks:           %v\n", schedGpuTasks)

		return nil
	},
}

var sealingSetWorkerModeCmd = &cli.Command{
	Name:  "set-worker-mode",
	Usage: "Set worker mode",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name: "address",
		},
		&cli.StringFlag{
			Name:  "mode",
			Value: "maintaining",
			Usage: "worker mode [maintaining | normal]",
		},
	},
	Action: func(cctx *cli.Context) error {
		address := cctx.String("address")
		mode := cctx.String("mode")

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		return nodeApi.SetWorkerMode(ctx, address, mode)
	},
}

var sealingSetWorkerReservedSpaceCmd = &cli.Command{
	Name:  "set-worker-reserved-space",
	Usage: "Set worker store's reserved space",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name: "address",
		},
		&cli.StringFlag{
			Name:  "store-id",
			Value: "",
		},
		&cli.Int64Flag{
			Name:  "reserved-space",
			Value: 0,
		},
	},
	Action: func(cctx *cli.Context) error {
		address := cctx.String("address")
		storeID := cctx.String("store-id")
		reserved := cctx.Int64("reserved-space")

		if "" == storeID {
			return nil
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		return nodeApi.SetWorkerReservedSpace(ctx, address, storeID, reserved)
	},
}

var scheduleEnableDebugCmd = &cli.Command{
	Name:      "sched-enable-debug",
	Usage:     "Enable more debug information of schedule",
	ArgsUsage: "[true|false]",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("expected 1 argument")
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		enable, _ := strconv.ParseBool(cctx.Args().First())

		return nodeApi.SetScheduleDebugEnable(ctx, enable)
	},
}
