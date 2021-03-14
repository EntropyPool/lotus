package main

import (
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"os"
	"strings"
	"time"
)

const (
	// Lord miner can do sealing, winning post, window post, it's fix machine. Lord miner is also master miner
	// Master miner can do winning post, window post, it's from slave miner
	// Slave miner just wait for sometime to be master miner. When current master miner is lost, the next will become master
	MinerLord   = "lord"
	MinerMaster = "master"
	MinerSlave  = "slave"
)

type candidateMiner struct {
	nativeRole  string
	runtimeRole string
	envValue    string
	mySelf      bool
}

type MultiMiner struct {
	candidates      []*candidateMiner
	iMLord          bool
	envValue        string
	masterIndex     int
	masterFailCount int
}

const multiMinerApiInfosEnvKey = "MULTI_MINER_API_INFOS"

func (multiMiner *MultiMiner) updateMultiMiner(cctx *cli.Context) error {
	env, ok := os.LookupEnv(multiMinerApiInfosEnvKey)
	if !ok {
		return xerrors.Errorf("%v is not defined", multiMinerApiInfosEnvKey)
	}

	if env == multiMiner.envValue {
		return nil
	}

	log.Infof("Update multi miner %v -> %v", multiMiner.envValue, env)

	multiMiner.envValue = env
	minerApiInfos := strings.Split(env, ";")
	multiMiner.candidates = []*candidateMiner{}

	for _, info := range minerApiInfos {
		role := MinerSlave
		apiInfo := info

		vars := strings.Split(info, ",")
		if 1 < len(vars) {
			apiInfo = vars[0]
			role = vars[1]
		}

		mySelf, err := lcli.StorageMinerIsMySelf(cctx, apiInfo)
		if err != nil {
			log.Errorf("Cannot check if it's myself %v\n", err)
			continue
		}

		if mySelf {
			if role == MinerLord {
				multiMiner.iMLord = true
			}
		}

		multiMiner.candidates = append(multiMiner.candidates, &candidateMiner{
			nativeRole:  role,
			runtimeRole: role,
			envValue:    apiInfo,
			mySelf:      mySelf,
		})
	}

	return nil
}

func newMultiMiner(cctx *cli.Context) (*MultiMiner, error) {
	multiMiner := &MultiMiner{}

	err := multiMiner.updateMultiMiner(cctx)
	if err != nil {
		return nil, err
	}

	return multiMiner, nil
}

func (multiMiner *MultiMiner) notifyMaster(cctx *cli.Context) error {
	var err error

	for index, candidate := range multiMiner.candidates {
		nerr := lcli.AnnounceMaster(cctx, candidate.envValue)
		if nerr != nil {
			log.Infof("Fail to announce master to [%v] %v: %v", index, candidate.envValue, nerr)
			err = nerr
		}
	}

	return err
}

func (multiMiner *MultiMiner) getCurrentNodeMasterIndex(cctx *cli.Context) (int, error) {
	for index, candidate := range multiMiner.candidates {
		err := lcli.CheckCurrentMaster(cctx, candidate.envValue)
		if err == nil {
			return index, nil
		}
	}

	return -1, xerrors.Errorf("None of my list is played as a master")
}

func (multiMiner *MultiMiner) selectAndCheckMaster(cctx *cli.Context) error {
	masterIndex, err := multiMiner.getCurrentNodeMasterIndex(cctx)
	if err == nil {
		if masterIndex != multiMiner.masterIndex {
			log.Infof("Master is switch from %v -> %v", multiMiner.masterIndex, masterIndex)
			multiMiner.masterIndex = masterIndex
			multiMiner.masterFailCount = 0
		}
	} else {
		log.Errorf("Fail to get current master index: %v", err)
	}

	currentMasterEnv := multiMiner.candidates[multiMiner.masterIndex].envValue

	err = lcli.CheckMaster(cctx, currentMasterEnv)
	if err != nil {
		multiMiner.masterFailCount += 1
	}

	if 6 < multiMiner.masterFailCount {
		multiMiner.masterIndex += 1
		multiMiner.masterIndex %= len(multiMiner.candidates)
		multiMiner.masterFailCount = 0
	}

	if multiMiner.candidates[multiMiner.masterIndex].mySelf {
		log.Infof("I'm master now, announce to others")
		return multiMiner.notifyMaster(cctx)
	}

	return nil
}

func (multiMiner *MultiMiner) keepaliveProcess(cctx *cli.Context) error {
	if multiMiner.iMLord {
		log.Infof("I'm lord, always announce I'm master")
		return multiMiner.notifyMaster(cctx)
	}
	return multiMiner.selectAndCheckMaster(cctx)
}

func MultiMinerRun(cctx *cli.Context) {
	var multiMiner *MultiMiner
	var err error
	ticker := time.NewTicker(20 * time.Second)

	log.Infof("Run multi miner elector")

waitForMiner:
	for {
		select {
		case <-ticker.C:
			if multiMiner == nil {
				multiMiner, err = newMultiMiner(cctx)
				if err == nil {
					log.Infof("Success to create multi miner")
					break waitForMiner
				}
			}
		}
	}

	for {
		select {
		case <-ticker.C:
			multiMiner.updateMultiMiner(cctx)
			err := multiMiner.keepaliveProcess(cctx)
			if err != nil {
				log.Errorf("Fail to keepalive to slave %v\n", err)
			}
		}
	}
}

var multiMinerConfigCmd = &cli.Command{
	Name:      "config",
	Usage:     "Config miner apis info for multi miner",
	ArgsUsage: "[miner_api_infos]",
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

		return nodeApi.SetEnvironment(ctx, multiMinerApiInfosEnvKey, cctx.Args().First())
	},
}
