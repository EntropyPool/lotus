package main

import (
	"encoding/json"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"
	"path/filepath"
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
	NativeRole  string `json:"native_role"`
	RuntimeRole string `json:"runtime_role"`
	EnvValue    string `json:"env_value"`
	mySelf      bool   `json:"my_self"`
}

type MultiMiner struct {
	Candidates      []*candidateMiner `json:"Candidates"`
	IMLord          bool              `json:"lord"`
	EnvValue        string            `json: "env_value"`
	masterIndex     int
	masterFailCount int
	rootPath        string
}

const multiMinerApiInfosEnvKey = "MULTI_MINER_API_INFOS"
const multiMinerMetaFile = "multiminerpeers.conf"

func (multiMiner *MultiMiner) updateMultiMiner(cctx *cli.Context) error {
	env, ok := os.LookupEnv(multiMinerApiInfosEnvKey)
	if !ok || env == "" {
		env = multiMiner.EnvValue
	}

	if env == "" {
		multiMiner.IMLord = true
		return xerrors.Errorf("%v is not empty", multiMinerApiInfosEnvKey)
	}

	log.Infof("Update multi miner %v -> %v", multiMiner.EnvValue, env)

	multiMiner.EnvValue = env
	minerApiInfos := strings.Split(env, ";")
	multiMiner.Candidates = []*candidateMiner{}
	multiMiner.IMLord = false

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
				multiMiner.IMLord = true
			}
		}

		multiMiner.Candidates = append(multiMiner.Candidates, &candidateMiner{
			NativeRole:  role,
			RuntimeRole: role,
			EnvValue:    apiInfo,
			mySelf:      mySelf,
		})
	}

	b, err := json.MarshalIndent(multiMiner, "", "  ")
	if err != nil {
		return xerrors.Errorf("marshaling storage config: %w", err)
	}

	metaFile := filepath.Join(multiMiner.rootPath, multiMinerMetaFile)
	if err := ioutil.WriteFile(metaFile, b, 0644); err != nil {
		return xerrors.Errorf("persisting storage metadata (%s): %w", filepath.Join(multiMiner.rootPath, multiMinerMetaFile), err)
	}

	return nil
}

func newMultiMiner(cctx *cli.Context, rootPath string) (*MultiMiner, error) {
	multiMiner := &MultiMiner{
		rootPath:   rootPath,
	}

	metaFile := filepath.Join(multiMiner.rootPath, multiMinerMetaFile)
	b, err := ioutil.ReadFile(metaFile)
	if err == nil {
		err = json.Unmarshal(b, multiMiner)
		if err != nil {
			log.Errorf("CANNOT parse %v", metaFile)
		}
	} else {
		log.Errorf("Fail to read %v", metaFile)
	}

	err = multiMiner.updateMultiMiner(cctx)
	if err != nil {
		log.Errorf("Fail to update multi miner")
	}

	return multiMiner, nil
}

func (multiMiner *MultiMiner) notifyMaster(cctx *cli.Context) error {
	var err error

	lcli.AnnounceMyselfAsMaster(cctx)

	for index, candidate := range multiMiner.Candidates {
		nerr := lcli.AnnounceMaster(cctx, candidate.EnvValue)
		if nerr != nil {
			log.Infof("Fail to announce master to [%v] %v: %v", index, candidate.EnvValue, nerr)
			err = nerr
		}
	}

	return err
}

func (multiMiner *MultiMiner) getCurrentNodeMasterIndex(cctx *cli.Context) (int, error) {
	for index, candidate := range multiMiner.Candidates {
		err := lcli.CheckCurrentMaster(cctx, candidate.EnvValue)
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

	currentMasterEnv := multiMiner.Candidates[multiMiner.masterIndex].EnvValue

	err = lcli.CheckMaster(cctx, currentMasterEnv)
	if err != nil {
		log.Errorf("Check master %v fail: %v [%v / %v]", currentMasterEnv, err, multiMiner.masterIndex, multiMiner.masterFailCount)
		multiMiner.masterFailCount += 1
	}

	if 6 < multiMiner.masterFailCount {
		multiMiner.masterIndex += 1
		multiMiner.masterIndex %= len(multiMiner.Candidates)
		multiMiner.masterFailCount = 0
	}

	if multiMiner.Candidates[multiMiner.masterIndex].mySelf {
		err = lcli.SetPlayAsMaster(cctx, true)
		if err != nil {
			log.Errorf("CANNOT set myself play as master: %v", err)
			return err
		}
		log.Debugf("I'm master now, announce to others")
		return multiMiner.notifyMaster(cctx)
	}

	lcli.SetPlayAsMaster(cctx, false)

	return nil
}

func (multiMiner *MultiMiner) keepaliveProcess(cctx *cli.Context) error {
	if multiMiner.IMLord || len(multiMiner.Candidates) == 0 {
		err := lcli.SetPlayAsLord(cctx, true)
		if err != nil {
			return err
		}
		err = lcli.SetPlayAsMaster(cctx, true)
		if err != nil {
			return err
		}
		log.Infof("I'm lord, always announce I'm master")
		return multiMiner.notifyMaster(cctx)
	}
	return multiMiner.selectAndCheckMaster(cctx)
}

func MultiMinerRun(cctx *cli.Context, rootPath string) {
	var multiMiner *MultiMiner
	ticker := time.NewTicker(20 * time.Second)

	log.Infof("Run multi miner elector")
	multiMiner, _ = newMultiMiner(cctx, rootPath)

	for {
		multiMiner.updateMultiMiner(cctx)
		err := multiMiner.keepaliveProcess(cctx)
		if err != nil {
			log.Errorf("Fail to keepalive to slave %v\n", err)
		}
		<-ticker.C
	}
}

var multiMinerConfigCmd = &cli.Command{
	Name:  "multi",
	Usage: "Config multi miner attributes",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "multi-miner-api-infos",
			Value: "",
			Usage: "miner apis separated by ';', in each value, you can use ',' to specific the miner as a lord. only lord can do sealing function",
		},
	},
	Action: func(cctx *cli.Context) error {
		minerApiInfos := cctx.String("multi-miner-api-infos")

		if "" == minerApiInfos {
			return xerrors.Errorf("multi-miner-api-infos should not empty")
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		return nodeApi.SetEnvironment(ctx, multiMinerApiInfosEnvKey, cctx.String("multi-miner-api-infos"))
	},
}
