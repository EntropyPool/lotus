package main

import (
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	lcli "github.com/filecoin-project/lotus/cli"
)

var setEnvCmd = &cli.Command{
	Name:  "set-env",
	Usage: "Set environment value at runtime",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "env-name",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "env-value",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := lcli.GetWorkerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		if err := api.SetEnvironment(ctx, cctx.String("env-name"), cctx.String("env-value")); err != nil {
			return xerrors.Errorf("SetEnvironment: %w", err)
		}

		return nil
	},
}

var unsetEnvCmd = &cli.Command{
	Name:  "unset-env",
	Usage: "Unset environment value at runtime",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "env-name",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := lcli.GetWorkerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		if err := api.UnsetEnvironment(ctx, cctx.String("env-name")); err != nil {
			return xerrors.Errorf("UnsetEnvironment: %w", err)
		}

		return nil
	},
}
