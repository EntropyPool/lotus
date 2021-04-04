package main

import (
	"time"

	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
)

type nonceTs struct {
	nonce       uint64
	firstAppear time.Time
}

type FeeAdjuster struct {
	maxFee   big.Int
	nonceTss map[address.Address]nonceTs
}

func (adjuster *FeeAdjuster) adjustFeeForAddress(cctx *cli.Context, address address.Address, name string) {
	ctx := lcli.ReqContext(cctx)

	api, acloser, err := lcli.GetFullNodeAPI(cctx)
	if err != nil {
		return
	}
	defer acloser()

	ts, err := api.ChainHead(ctx)
	if err != nil {
		return
	}

	pending, err := api.MpoolPending(ctx, ts.Key())
	if err != nil {
		return
	}

	msgCount := 0
	nts := adjuster.nonceTss[address]
	var minNonce uint64 = 0

	for _, p := range pending {
		if p.Message.From == address {
			msgCount += 1
			if p.Message.Nonce < nts.nonce {
				minNonce = p.Message.Nonce
			}
		}
	}

	if nts.nonce == 0 || nts.nonce < minNonce {
		nts.nonce = minNonce
		nts.firstAppear = time.Now()
	}

	adjuster.nonceTss[address] = nts

	if false && msgCount < 20 && time.Now().Before(nts.firstAppear.Add(20*time.Minute)) {
		log.Infof("%v | %v only have %v messages, and %v + 20Min < %v, wait for a moment",
			name, address, msgCount, nts.firstAppear, time.Now())
		return
	}

	b, err := api.WalletBalance(ctx, address)
	if err != nil {
		log.Errorf("%s\t%s: error getting balance: %s\n", name, address, err)
		return
	}

	k, err := api.StateAccountKey(ctx, address, types.EmptyTSK)
	if err != nil {
		log.Errorf("%s\t%s: error getting account key: %s\n", name, address, err)
		return
	}

	log.Infof("balance: %v, account key: %v", b, k)
}

func (adjuster *FeeAdjuster) adjustFeeForAddresses(cctx *cli.Context, addresses map[address.Address]string) {
	for addr, name := range addresses {
		adjuster.adjustFeeForAddress(cctx, addr, name)
	}
}

func (adjuster *FeeAdjuster) adjustFee(cctx *cli.Context) {
	nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
	if err != nil {
		log.Errorf("cannot get storage miner api: %v", err)
		return
	}
	defer closer()

	api, acloser, err := lcli.GetFullNodeAPI(cctx)
	if err != nil {
		log.Errorf("cannot get node miner api: %v", err)
		return
	}
	defer acloser()

	ctx := lcli.ReqContext(cctx)

	maddr, err := nodeApi.ActorAddress(ctx)
	if err != nil {
		log.Errorf("cannot get miner address: %v", err)
		return
	}

	mi, err := api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		log.Errorf("cannot state miner %v info: %v", maddr, err)
		return
	}

	ac, err := nodeApi.ActorAddressConfig(ctx)
	if err != nil {
		log.Errorf("cannot get miner %v config: %v", maddr, err)
		return
	}

	addrs := map[address.Address]string{}
	for _, ca := range mi.ControlAddresses {
		addrs[ca] = "PoStControl"
	}

	for _, ca := range ac.PreCommitControl {
		ca, err := api.StateLookupID(ctx, ca, types.EmptyTSK)
		if err != nil {
			return
		}
		addrs[ca] = "PreCommitControl"
	}

	for _, ca := range ac.CommitControl {
		ca, err := api.StateLookupID(ctx, ca, types.EmptyTSK)
		if err != nil {
			return
		}
		addrs[ca] = "CommitControl"
	}

	addrs[mi.Worker] = "Worker"

	for addr, _ := range addrs {
		if _, ok := adjuster.nonceTss[addr]; !ok {
			adjuster.nonceTss[addr] = nonceTs{
				nonce:       0,
				firstAppear: time.Now(),
			}
		}
	}

	adjuster.adjustFeeForAddresses(cctx, addrs)
}

func FeeAdjusterRun(cctx *cli.Context) {
	ticker := time.NewTicker(2 * time.Minute)

	adjuster := &FeeAdjuster{
		maxFee:   types.FromFil(2),
		nonceTss: map[address.Address]nonceTs{},
	}

	for {
		select {
		case <-ticker.C:
			adjuster.adjustFee(cctx)
		}
	}
}
