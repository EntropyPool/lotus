package main

import (
	"fmt"
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

func (multiMiner *MultiMiner) updateMultiMiner(cctx *cli.Context) error {
	env, ok := os.LookupEnv("MULTI_MINER_API_INFOS")
	if !ok {
		return xerrors.Errorf("MULTI_MINER_API_INFOS is not defined")
	}

	if env == multiMiner.envValue {
		return nil
	}

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
			fmt.Printf("Cannot check if it's myself %v\n", err)
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

	for _, candidate := range multiMiner.candidates {
		if candidate.mySelf {
			continue
		}

		nerr := lcli.AnnounceMaster(cctx, candidate.envValue)
		if nerr != nil {
			err = nerr
		}
	}

	return err
}

func (multiMiner *MultiMiner) selectAndCheckMaster(cctx *cli.Context) error {
	err := lcli.CheckMaster(cctx, multiMiner.candidates[multiMiner.masterIndex].envValue)
	if err != nil {
		multiMiner.masterFailCount += 1
	}

	if 6 < multiMiner.masterFailCount {
		multiMiner.masterIndex += 1
		multiMiner.masterIndex %= len(multiMiner.candidates)
		multiMiner.masterFailCount = 0
	}

	if multiMiner.candidates[multiMiner.masterIndex].mySelf {
		return lcli.AnnounceMaster(cctx, multiMiner.candidates[multiMiner.masterIndex].envValue)
	}

	return nil
}

func (multiMiner *MultiMiner) keepaliveProcess(cctx *cli.Context) error {
	if multiMiner.iMLord {
		return multiMiner.notifyMaster(cctx)
	}
	return multiMiner.selectAndCheckMaster(cctx)
}

func MultiMinerRun(cctx *cli.Context) {
	var multiMiner *MultiMiner
	var err error
	ticker := time.NewTicker(20 * time.Second)

waitForMiner:
	for {
		select {
		case <-ticker.C:
			if multiMiner == nil {
				multiMiner, err = newMultiMiner(cctx)
				if err == nil {
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
				fmt.Printf("Fail to keepalive to slave %v\n", err)
			}
		}
	}
}
