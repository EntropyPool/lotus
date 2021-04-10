package cli

import (
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	miner2 "github.com/filecoin-project/lotus/chain/actors/builtin/miner"

	"github.com/filecoin-project/lotus/api"
	cliutil "github.com/filecoin-project/lotus/cli/util"
)

var log = logging.Logger("cli")

// custom CLI error

type ErrCmdFailed struct {
	msg string
}

func (e *ErrCmdFailed) Error() string {
	return e.msg
}

func NewCliError(s string) error {
	return &ErrCmdFailed{s}
}

// ApiConnector returns API instance
type ApiConnector func() api.FullNode

func GetAPIInfoWithEnvValue(ctx *cli.Context, envVal string) (cliutil.APIInfo, error) {
	return cliutil.ParseApiInfo(envVal), nil
}

func GetRawAPIWithAPIInfo(ctx *cli.Context, ainfo cliutil.APIInfo) (string, http.Header, error) {
	addr, err := ainfo.DialArgs()
	if err != nil {
		return "", nil, xerrors.Errorf("could not get DialArgs: %w", err)
	}
	return addr, ainfo.AuthHeader(), nil
}

func GetFullNodeServices(ctx *cli.Context) (ServicesAPI, error) {
	if tn, ok := ctx.App.Metadata["test-services"]; ok {
		return tn.(ServicesAPI), nil
	}

	api, c, err := GetFullNodeAPI(ctx)
	if err != nil {
		return nil, err
	}

	return &ServicesImpl{api: api, closer: c}, nil
}

var GetAPIInfo = cliutil.GetAPIInfo
var GetRawAPI = cliutil.GetRawAPI
var GetAPI = cliutil.GetAPI

var DaemonContext = cliutil.DaemonContext
var ReqContext = cliutil.ReqContext

var GetFullNodeAPI = cliutil.GetFullNodeAPI
var GetGatewayAPI = cliutil.GetGatewayAPI

var GetStorageMinerAPI = cliutil.GetStorageMinerAPI
var GetWorkerAPI = cliutil.GetWorkerAPI

func StorageMinerIsMySelf(ctx *cli.Context, apiInfo string) (bool, error) {
	ainfo, err := GetAPIInfo(ctx, repo.StorageMiner)
	if err != nil {
		return false, err
	}

	avInfo, err := GetAPIInfoWithEnvValue(ctx, apiInfo)
	if err != nil {
		return false, err
	}

	if ainfo.Addr == avInfo.Addr {
		return true, nil
	}

	return false, nil
}

func GetStorageMinerAPIWithAPIInfo(ctx *cli.Context, apiInfo string, opts ...GetStorageMinerOption) (api.StorageMiner, jsonrpc.ClientCloser, error) {
	var options GetStorageMinerOptions
	for _, opt := range opts {
		opt(&options)
	}

	if tn, ok := ctx.App.Metadata["testnode-storage"]; ok {
		return tn.(api.StorageMiner), func() {}, nil
	}

	ainfo, err := GetAPIInfoWithEnvValue(ctx, apiInfo)
	if err != nil {
		return nil, nil, err
	}

	addr, headers, err := GetRawAPIWithAPIInfo(ctx, ainfo)
	if err != nil {
		return nil, nil, err
	}

	if options.PreferHttp {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, nil, xerrors.Errorf("parsing miner api URL: %w", err)
		}

		switch u.Scheme {
		case "ws":
			u.Scheme = "http"
		case "wss":
			u.Scheme = "https"
		}

		addr = u.String()
	}

	return client.NewStorageMinerRPC(ctx.Context, addr, headers)
}

func CheckMaster(ctx *cli.Context, apiInfo string) error {
	minerApi, closer, err := GetStorageMinerAPIWithAPIInfo(ctx, apiInfo)
	if err != nil {
		return err
	}
	defer closer()

	return minerApi.CheckMaster(ctx.Context)
}

func SetPlayAsMaster(ctx *cli.Context, master bool) error {
	addr, _, err := GetRawAPI(ctx, repo.StorageMiner)
	if err != nil {
		return err
	}

	minerApi, closer, err := GetStorageMinerAPI(ctx)
	if err != nil {
		return err
	}
	defer closer()

	return minerApi.SetPlayAsMaster(ctx.Context, master, addr)
}

func SetPlayAsLord(ctx *cli.Context, lord bool) error {
	minerApi, closer, err := GetStorageMinerAPI(ctx)
	if err != nil {
		return err
	}
	defer closer()

	return minerApi.SetPlayAsLord(ctx.Context, lord)
}

func CheckCurrentMaster(ctx *cli.Context, apiInfo string) error {
	minerApi, closer, err := GetStorageMinerAPI(ctx)
	if err != nil {
		return err
	}
	defer closer()

	ainfo, err := GetAPIInfoWithEnvValue(ctx, apiInfo)
	if err != nil {
		return err
	}

	addr, _, err := GetRawAPIWithAPIInfo(ctx, ainfo)
	if err != nil {
		return err
	}

	return minerApi.CheckCurrentMaster(ctx.Context, addr)
}

func AnnounceMaster(ctx *cli.Context, apiInfo string) error {
	minerApi, closer, err := GetStorageMinerAPIWithAPIInfo(ctx, apiInfo)
	if err != nil {
		return err
	}
	defer closer()

	addrMaster, headersMaster, err := GetRawAPI(ctx, repo.StorageMiner)
	if err != nil {
		return err
	}

	ainfo, err := GetAPIInfoWithEnvValue(ctx, apiInfo)
	if err != nil {
		return err
	}

	addrSlave, headersSlave, err := GetRawAPIWithAPIInfo(ctx, ainfo)
	if err != nil {
		return err
	}

	return minerApi.AnnounceMaster(ctx.Context, addrMaster, headersMaster, addrSlave, headersSlave)
}

func UpdateChainEndpoints(ctx *cli.Context, apiInfos []string) error {
	minerApi, closer, err := GetStorageMinerAPI(ctx)
	if err != nil {
		return err
	}
	defer closer()

	endpoints := map[string]http.Header{}
	for _, ai := range apiInfos {
		ainfo, err := GetAPIInfoWithEnvValue(ctx, ai)
		if err != nil {
			return err
		}

		addr, headers, err := GetRawAPIWithAPIInfo(ctx, ainfo)
		if err != nil {
			return err
		}
		endpoints[addr] = headers
	}

	if len(endpoints) == 0 {
		return xerrors.Errorf("empty chain endpoints info")
	}

	return minerApi.UpdateChainEndpoints(ctx.Context, endpoints)
}

func CheckWindowPoSt(ctx *cli.Context, deadline uint64) ([]miner2.SubmitWindowedPoStParams, error) {
	minerApi, closer, err := GetStorageMinerAPI(ctx)
	if err != nil {
		return nil, err
	}
	defer closer()

	return minerApi.CheckWindowPoSt(ctx.Context, deadline)
}

func AnnounceMyselfAsMaster(ctx *cli.Context) error {
	minerApi, closer, err := GetStorageMinerAPI(ctx)
	if err != nil {
		return err
	}
	defer closer()

	addr, headers, err := GetRawAPI(ctx, repo.StorageMiner)
	if err != nil {
		return err
	}
	return minerApi.AnnounceMaster(ctx.Context, addr, headers, addr, headers)
}

var CommonCommands = []*cli.Command{
	netCmd,
	authCmd,
	logCmd,
	waitApiCmd,
	fetchParamCmd,
	pprofCmd,
	VersionCmd,
}

var Commands = []*cli.Command{
	WithCategory("basic", sendCmd),
	WithCategory("basic", walletCmd),
	WithCategory("basic", clientCmd),
	WithCategory("basic", multisigCmd),
	WithCategory("basic", paychCmd),
	WithCategory("developer", authCmd),
	WithCategory("developer", mpoolCmd),
	WithCategory("developer", stateCmd),
	WithCategory("developer", chainCmd),
	WithCategory("developer", logCmd),
	WithCategory("developer", waitApiCmd),
	WithCategory("developer", fetchParamCmd),
	WithCategory("network", netCmd),
	WithCategory("network", syncCmd),
	pprofCmd,
	VersionCmd,
}

func WithCategory(cat string, cmd *cli.Command) *cli.Command {
	cmd.Category = strings.ToUpper(cat)
	return cmd
}
