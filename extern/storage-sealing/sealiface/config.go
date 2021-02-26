package sealiface

import (
	"github.com/filecoin-project/go-state-types/abi"
	"time"
)

// this has to be in a separate package to not make lotus API depend on filecoin-ffi

type Config struct {
	// 0 = no limit
	MaxWaitDealsSectors uint64

	// includes failed, 0 = no limit
	MaxSealingSectors uint64

	// includes failed, 0 = no limit
	MaxSealingSectorsForDeals uint64

	WaitDealsDelay time.Duration

	AlwaysKeepUnsealedCopy bool

	PreferSectorOnChain bool

	EnableAutoPledge bool

	AutoPledgeBalanceThreshold abi.TokenAmount
}
