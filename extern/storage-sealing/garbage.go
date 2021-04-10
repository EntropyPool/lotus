package sealing

import (
	"context"

	"golang.org/x/xerrors"

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
				m.PledgeSector(ctx)
				time.Sleep(time.Second)
			}
		}
	}
}

func (m *Sealing) PledgeSector(ctx context.Context) (storage.SectorRef, error) {
	m.inputLk.Lock()
	defer m.inputLk.Unlock()

	cfg, err := m.getConfig()
	if err != nil {
		return storage.SectorRef{}, xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() >= cfg.MaxSealingSectors {
			return storage.SectorRef{}, xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	spt, err := m.currentSealProof(ctx)
	if err != nil {
		return storage.SectorRef{}, xerrors.Errorf("getting seal proof type: %w", err)
	}

	sid, err := m.sc.Next()
	if err != nil {
		return storage.SectorRef{}, xerrors.Errorf("generating sector number: %w", err)
	}
	sectorID := m.minerSector(spt, sid)
	err = m.sealer.NewSector(ctx, sectorID)
	if err != nil {
		return storage.SectorRef{}, xerrors.Errorf("notifying sealer of the new sector: %w", err)
	}

	log.Infof("Creating CC sector %d", sid)
	return sectorID, m.sectors.Send(uint64(sid), SectorStartCC{
		ID:         sid,
		SectorType: spt,
	})
}
