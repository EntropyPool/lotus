package main

import (
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"strings"
)

type ChainEndpoints struct {
	apiInfos []string
}

const minerChainEndpointsEnvKey = "MINER_CHAIN_ENDPOINTS"
const minerChainEndpointsMeta = "minerchainendpoints.conf"

func ChainEndpointsWatcher(cctx *cli.Context, rootPath string) {

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
