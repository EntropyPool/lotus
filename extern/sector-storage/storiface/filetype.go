package storiface

import (
	"fmt"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
)

const (
	FTUnsealed SectorFileType = 1 << iota
	FTSealed
	FTCache

	FileTypes = iota
)

var PathTypes = []SectorFileType{FTUnsealed, FTSealed, FTCache}

const (
	FTNone SectorFileType = 0
)

const FSOverheadDen = 10

var FSOverheadSeal = map[SectorFileType]int{ // 10x overheads
	FTUnsealed: FSOverheadDen,
	FTSealed:   FSOverheadDen,
	FTCache:    141, // 11 layers + D(2x ssize) + C + R
}

var FsOverheadFinalized = map[SectorFileType]int{
	FTUnsealed: FSOverheadDen,
	FTSealed:   FSOverheadDen,
	FTCache:    2,
}

type SectorFileType int

func (t SectorFileType) String() string {
	switch t {
	case FTUnsealed:
		return "unsealed"
	case FTSealed:
		return "sealed"
	case FTCache:
		return "cache"
	default:
		return fmt.Sprintf("<unknown %d>", t)
	}
}

func (t SectorFileType) Has(singleType SectorFileType) bool {
	return t&singleType == singleType
}

func (t SectorFileType) SealSpaceUse(ssize abi.SectorSize) (uint64, error) {
	var need uint64
	for _, pathType := range PathTypes {
		if !t.Has(pathType) {
			continue
		}

		oh, ok := FSOverheadSeal[pathType]
		if !ok {
			return 0, xerrors.Errorf("no seal overhead info for %s", pathType)
		}

		need += uint64(oh) * uint64(ssize) / FSOverheadDen
	}

	return need, nil
}

func (t SectorFileType) All() [FileTypes]bool {
	var out [FileTypes]bool

	for i := range out {
		out[i] = t&(1<<i) > 0
	}

	return out
}

type SectorOSSInfo struct {
	URL         string
	LandedDir   string
	AccessKey   string
	SecretKey   string
	BucketName  string
	SectorName  string
	Prefix      string
	Region      string
	MultiRanges bool
}

type SectorPath struct {
	Path    string
	Oss     bool
	Private interface{}
	OssInfo SectorOSSInfo
}

type SectorPaths struct {
	ID abi.SectorID

	Unsealed SectorPath
	Sealed   SectorPath
	Cache    SectorPath
}

func ParseSectorID(baseName string) (abi.SectorID, error) {
	var n abi.SectorNumber
	var mid abi.ActorID
	read, err := fmt.Sscanf(baseName, "s-t0%d-%d", &mid, &n)
	if err != nil {
		return abi.SectorID{}, xerrors.Errorf("sscanf sector name ('%s'): %w", baseName, err)
	}

	if read != 2 {
		return abi.SectorID{}, xerrors.Errorf("parseSectorID expected to scan 2 values, got %d", read)
	}

	return abi.SectorID{
		Miner:  mid,
		Number: n,
	}, nil
}

func SectorName(sid abi.SectorID) string {
	return fmt.Sprintf("s-t0%d-%d", sid.Miner, sid.Number)
}

func PathByType(sps SectorPaths, fileType SectorFileType) string {
	switch fileType {
	case FTUnsealed:
		return sps.Unsealed.Path
	case FTSealed:
		return sps.Sealed.Path
	case FTCache:
		return sps.Cache.Path
	}

	panic("requested unknown path type")
}

func SetPathByType(sps *SectorPaths, fileType SectorFileType, p string) {
	switch fileType {
	case FTUnsealed:
		sps.Unsealed.Path = p
	case FTSealed:
		sps.Sealed.Path = p
	case FTCache:
		sps.Cache.Path = p
	}
}

func PathExtByType(sps SectorPaths, fileType SectorFileType) SectorPath {
	switch fileType {
	case FTUnsealed:
		return sps.Unsealed
	case FTSealed:
		return sps.Sealed
	case FTCache:
		return sps.Cache
	}

	panic("requested unknown path type")
}

func SetPathExtByType(sps *SectorPaths, fileType SectorFileType, oss bool, priv interface{}, ossInfo SectorOSSInfo) {
	switch fileType {
	case FTUnsealed:
		sps.Unsealed.Oss = oss
		sps.Unsealed.Private = priv
		sps.Unsealed.OssInfo = ossInfo
	case FTSealed:
		sps.Sealed.Oss = oss
		sps.Sealed.Private = priv
		sps.Sealed.OssInfo = ossInfo
	case FTCache:
		sps.Cache.Oss = oss
		sps.Cache.Private = priv
		sps.Cache.OssInfo = ossInfo
	}
}
