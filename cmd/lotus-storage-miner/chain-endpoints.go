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

type ChainEndpoints struct {
	apiInfos map[string]struct{}
}

const minerChainEndpointsEnvKey = "MINER_CHAIN_ENDPOINTS"
const minerChainEndpointsMeta = "minerchainendpoints.conf"

func updateAndNotifyChainEndpoints(cctx *cli.Context, rootPath string, chp *ChainEndpoints) {
	new := false

	env := os.Getenv(minerChainEndpointsEnvKey)
	if len(env) == 0 {
		return
	}

	eps := strings.Split(env, ";")
	for _, ep := range eps {
		if _, ok := chp.apiInfos[ep]; ok {
			continue
		}
		chp.apiInfos[ep] = struct{}{}
		new = true
	}

	if !new {
		return
	}

	ainfos := []string{}
	for ai, _ := range chp.apiInfos {
		ainfos = append(ainfos, ai)
	}

	err := lcli.UpdateChainEndpoints(cctx, ainfos)
	if err != nil {
		log.Errorf("fail to update chain endpoints: %v", err)
		return
	}

	b, _ := json.Marshal(chp.apiInfos)
	ioutil.WriteFile(filepath.Join(rootPath, minerChainEndpointsMeta), b, 0666)
}

func ChainEndpointsWatcher(cctx *cli.Context, rootPath string) {
	chp := &ChainEndpoints{
		apiInfos: map[string]struct{}{},
	}

	b, err := ioutil.ReadFile(filepath.Join(rootPath, minerChainEndpointsMeta))
	if err != nil {
		json.Unmarshal(b, &chp.apiInfos)
	}

	ticker := time.NewTicker(5 * time.Minute)
	for {
		updateAndNotifyChainEndpoints(cctx, rootPath, chp)
		<-ticker.C
	}
}

var minerChainEndpointsCmd = &cli.Command{
	Name:  "chain-endpoints",
	Usage: "Config miner chain endpoints for lotus high available",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "append-endpoints",
			Usage: "append chain endpoints separated by ';'",
		},
		&cli.StringFlag{
			Name:  "set-endpoints",
			Usage: "replace chain endpoints separated by ';'",
		},
	},
	Action: func(cctx *cli.Context) error {
		replace := false
		endpoints := cctx.String("append-endpoints")
		if len(endpoints) == 0 {
			endpoints = cctx.String("set-endpoints")
			replace = true
		}
		if len(endpoints) == 0 {
			return xerrors.Errorf("empty endpoints environment")
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		if !replace {
			old, err := nodeApi.GetEnvironment(ctx, minerChainEndpointsEnvKey)
			if err != nil {
				return xerrors.Errorf("cannot get environment %v: %v", minerChainEndpointsEnvKey, err)
			}
			endpoints = strings.Join([]string{endpoints}, old)
		}

		return nodeApi.SetEnvironment(ctx, minerChainEndpointsEnvKey, endpoints)
	},
}
