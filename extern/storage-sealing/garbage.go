package sealing

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/specs-storage/storage"
	"time"
)

func (m *Sealing) AutoPledgeTask(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-ticker.C:
			cfg, err := m.getConfig()
			if err != nil {
				break
			}
			if !cfg.EnableAutoPledge {
				break
			}
			tok, _, err := m.api.ChainHead(ctx)
			if err != nil {
				log.Errorf("autoPledge: api error, not proceeding: %+v", err)
				break
			}

			mi, err := m.api.StateMinerInfo(ctx, m.maddr, tok)
			if err != nil {
				log.Errorf("autoPledge: api error, not proceeding: %+v", err)
				break
			}
			from, balance, err := m.addrSel(ctx, mi, api.PreCommitAddr,
				cfg.AutoPledgeBalanceThreshold,
				cfg.AutoPledgeBalanceThreshold)
			if err != nil {
				log.Infof("autoPledge: balance from address error: %+v", err)
				break
			}
			if balance == cfg.AutoPledgeBalanceThreshold {
				log.Infof("autoPledge: are you kidding me? balance is totally same?")
				break
			}
			if big.Cmp(balance, cfg.AutoPledgeBalanceThreshold) < 0 {
				log.Infof("autoPledge: %v < %v [%v]", balance, cfg.AutoPledgeBalanceThreshold, from)
				break
			}
			sealings := m.sealer.PledgedJobs(ctx)
			log.Infof("autoPledge: pledge %+v sectors [%v / %v FIL in %v]", sealings, cfg.AutoPledgeBalanceThreshold, balance, from)
			for i := 0; i < sealings; i++ {
				m.PledgeSector()
				time.Sleep(time.Second)
			}
		}
	}
}

func (m *Sealing) pledgeSector(ctx context.Context, sectorID storage.SectorRef, existingPieceSizes []abi.UnpaddedPieceSize, sizes ...abi.UnpaddedPieceSize) ([]abi.PieceInfo, error) {
	if len(sizes) == 0 {
		return nil, nil
	}

	log.Infof("Pledge %d, contains %+v", sectorID, existingPieceSizes)

	out := make([]abi.PieceInfo, len(sizes))
	for i, size := range sizes {
		ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, NewNullReader(size))
		if err != nil {
			return nil, xerrors.Errorf("add piece: %w", err)
		}

		existingPieceSizes = append(existingPieceSizes, size)

		out[i] = ppi
	}

	return out, nil
}

func (m *Sealing) PledgeSector() error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() >= cfg.MaxSealingSectors {
			return xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		spt, err := m.currentSealProof(ctx)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		size, err := spt.SectorSize()
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		sid, err := m.sc.Next()
		if err != nil {
			log.Errorf("%+v", err)
			return
		}
		sectorID := m.minerSector(spt, sid)
		err = m.sealer.NewSector(ctx, sectorID)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		pieces, err := m.pledgeSector(ctx, sectorID, []abi.UnpaddedPieceSize{}, abi.PaddedPieceSize(size).Unpadded())
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		ps := make([]Piece, len(pieces))
		for idx := range ps {
			ps[idx] = Piece{
				Piece:    pieces[idx],
				DealInfo: nil,
			}
		}

		if err := m.newSectorCC(ctx, sid, ps); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}
