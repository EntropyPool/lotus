package stores

import (
	"fmt"
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"math/bits"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mitchellh/go-homedir"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type StoragePath struct {
	ID     ID
	Weight uint64

	LocalPath string

	CanSeal  bool
	CanStore bool
}

// LocalStorageMeta [path]/sectorstore.json
type LocalStorageMeta struct {
	ID ID

	// A high weight means data is more likely to be stored in this path
	Weight uint64 // 0 = readonly

	// Intermediate data for the sealing process will be stored here
	CanSeal bool

	// Finalized sectors that will be proved over time will be stored here
	CanStore bool

	Oss     bool
	OssInfo StorageOSSInfo
}

// StorageConfig .lotusstorage/storage.json
type StorageConfig struct {
	StoragePaths []LocalPath
}

type LocalPath struct {
	Path string
}

type LocalStorage interface {
	GetStorage() (StorageConfig, error)
	SetStorage(func(*StorageConfig)) error

	Stat(path string) (fsutil.FsStat, error)

	// returns real disk usage for a file/directory
	// os.ErrNotExit when file doesn't exist
	DiskUsage(path string) (int64, error)
}

const MetaFile = "sectorstore.json"

var PathTypes = []storiface.SectorFileType{storiface.FTUnsealed, storiface.FTSealed, storiface.FTCache}

type Local struct {
	localStorage LocalStorage
	index        SectorIndex
	urls         []string

	paths map[ID]*path

	localLk sync.RWMutex
}

type path struct {
	local string // absolute local path

	reserved     int64
	reservations map[abi.SectorID]storiface.SectorFileType

	oss       bool
	ossInfo   StorageOSSInfo
	ossClient *OSSClient
}

func (p *path) stat(ls LocalStorage) (fsutil.FsStat, error) {
	stat, err := ls.Stat(p.local)
	if err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("stat %s: %w", p.local, err)
	}

	stat.Reserved = p.reserved

	for id, ft := range p.reservations {
		for _, fileType := range storiface.PathTypes {
			if fileType&ft == 0 {
				continue
			}

			sp := p.sectorPath(id, fileType)

			used, err := ls.DiskUsage(sp)
			if err == os.ErrNotExist {
				p, ferr := tempFetchDest(sp, false)
				if ferr != nil {
					return fsutil.FsStat{}, ferr
				}

				used, err = ls.DiskUsage(p)
			}
			if err != nil {
				log.Errorf("getting disk usage of '%s': %+v", p.sectorPath(id, fileType), err)
				continue
			}

			stat.Reserved -= used
		}
	}

	if stat.Reserved < 0 {
		log.Warnf("negative reserved storage: p.reserved=%d, reserved: %d", p.reserved, stat.Reserved)
		stat.Reserved = 0
	}

	stat.Available -= stat.Reserved
	if stat.Available < 0 {
		stat.Available = 0
	}

	return stat, err
}

func (p *path) sectorPath(sid abi.SectorID, fileType storiface.SectorFileType) string {
	return filepath.Join(p.local, fileType.String(), storiface.SectorName(sid))
}

func NewLocal(ctx context.Context, ls LocalStorage, index SectorIndex, urls []string) (*Local, error) {
	l := &Local{
		localStorage:   ls,
		index:          index,
		urls:           urls,
		paths:          map[ID]*path{},
	}
	return l, l.open(ctx)
}

func (st *Local) UpdatePath(ctx context.Context, p string) error {
	st.localLk.Lock()
	defer st.localLk.Unlock()

	mb, err := ioutil.ReadFile(filepath.Join(p, MetaFile))
	if err != nil {
		return xerrors.Errorf("reading storage metadata for %s: %w", p, err)
	}

	var meta LocalStorageMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return xerrors.Errorf("unmarshalling storage metadata for %s: %w", p, err)
	}

	oldPath, ok := st.paths[meta.ID]
	if !ok {
		return xerrors.Errorf("cannot find storage %v", meta.ID)
	}

	newPath := &path{
		local:        p,
		reserved:     oldPath.reserved,
		reservations: oldPath.reservations,
		oss:          meta.Oss,
		ossInfo:      meta.OssInfo,
		ossClient:    oldPath.ossClient,
	}

	fst, err := newPath.stat(st.localStorage)
	if err != nil {
		return err
	}

	err = st.index.StorageAttach(ctx, StorageInfo{
		ID:       meta.ID,
		URLs:     st.urls,
		Weight:   meta.Weight,
		CanSeal:  meta.CanSeal,
		CanStore: meta.CanStore,
		Oss:      meta.Oss,
		OssInfo:  meta.OssInfo,
	}, fst)
	if err != nil {
		return xerrors.Errorf("update storage in index: %w", err)
	}

	st.paths[meta.ID] = newPath

	return nil
}

func (st *Local) OpenPath(ctx context.Context, p string) error {
	st.localLk.Lock()
	defer st.localLk.Unlock()

	mb, err := ioutil.ReadFile(filepath.Join(p, MetaFile))
	if err != nil {
		return xerrors.Errorf("reading storage metadata for %s: %w", p, err)
	}

	var meta LocalStorageMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return xerrors.Errorf("unmarshalling storage metadata for %s: %w", p, err)
	}

	// TODO: Check existing / dedupe

	out := &path{
		local: p,

		reserved:     0,
		reservations: map[abi.SectorID]storiface.SectorFileType{},

		oss: meta.Oss,
	}

	fst, err := out.stat(st.localStorage)
	if err != nil {
		return err
	}

	err = st.index.StorageAttach(ctx, StorageInfo{
		ID:       meta.ID,
		URLs:     st.urls,
		Weight:   meta.Weight,
		CanSeal:  meta.CanSeal,
		CanStore: meta.CanStore,
		Oss:      meta.Oss,
		OssInfo:  meta.OssInfo,
	}, fst)
	if err != nil {
		return xerrors.Errorf("declaring storage in index: %w", err)
	}

	if meta.Oss {
		cli, ossErr := NewOSSClient(meta.OssInfo)
		if ossErr != nil {
			return xerrors.Errorf("create oss client: %w", ossErr)
		}
		out.ossClient = cli
		out.ossInfo = meta.OssInfo
		err = st.declareSectorsFromOss(ctx, cli, meta.ID, meta.OssInfo.CanWrite, p)
	} else {
		err = st.declareSectors(ctx, p, meta.ID, meta.CanStore)
	}

	if err != nil {
		return xerrors.Errorf("open path: %w", err)
	}

	st.paths[meta.ID] = out

	return nil
}

func (st *Local) open(ctx context.Context) error {
	cfg, err := st.localStorage.GetStorage()
	if err != nil {
		return xerrors.Errorf("getting local storage config: %w", err)
	}

	for _, path := range cfg.StoragePaths {
		err := st.OpenPath(ctx, path.Path)
		if err != nil {
			return xerrors.Errorf("opening path %s: %w", path.Path, err)
		}
	}

	go st.reportHealth(ctx)

	return nil
}

func (st *Local) Redeclare(ctx context.Context) error {
	st.localLk.Lock()
	defer st.localLk.Unlock()

	for id, p := range st.paths {
		mb, err := ioutil.ReadFile(filepath.Join(p.local, MetaFile))
		if err != nil {
			return xerrors.Errorf("reading storage metadata for %s: %w", p.local, err)
		}

		var meta LocalStorageMeta
		if err := json.Unmarshal(mb, &meta); err != nil {
			return xerrors.Errorf("unmarshalling storage metadata for %s: %w", p.local, err)
		}

		fst, err := p.stat(st.localStorage)
		if err != nil {
			return err
		}

		if id != meta.ID {
			log.Errorf("storage path ID changed: %s; %s -> %s", p.local, id, meta.ID)
			continue
		}

		err = st.index.StorageAttach(ctx, StorageInfo{
			ID:       id,
			URLs:     st.urls,
			Weight:   meta.Weight,
			CanSeal:  meta.CanSeal,
			CanStore: meta.CanStore,
			Oss:      meta.Oss,
			OssInfo:  meta.OssInfo,
		}, fst)
		if err != nil {
			return xerrors.Errorf("redeclaring storage in index: %w", err)
		}

		if meta.Oss {
			cli, ossErr := NewOSSClient(meta.OssInfo)
			if ossErr != nil {
				return xerrors.Errorf("create oss client: %w", ossErr)
			}
			p.ossClient = cli
			p.ossInfo = meta.OssInfo
			err = st.declareSectorsFromOss(ctx, cli, id, meta.OssInfo.CanWrite, p.local)
		} else {
			err = st.declareSectors(ctx, p.local, meta.ID, meta.CanStore)
		}

		if err != nil {
			return xerrors.Errorf("redeclaring sectors: %w", err)
		}
	}

	return nil
}

func (st *Local) NotifySectorProving(ctx context.Context, sector storage.SectorRef) error {
	for id, _ := range st.paths {
		ssize, err := sector.ProofType.SectorSize()
		if err != nil {
			return err
		}
		_, err = st.index.StorageFindSector(ctx, sector.ID, storiface.FTCache|storiface.FTSealed, ssize, false)
		if err == nil {
			return nil
		}
		if err := st.index.StorageDeclareSector(ctx, id, sector.ID, storiface.FTCache|storiface.FTSealed, true); err != nil {
			log.Errorf("declare sector proving %d -> %s: %w", sector, id, err)
			continue
		}
		return nil
	}
	return xerrors.Errorf("cannot found suitable path to declare sector %v", sector)
}

func (st *Local) declareSectorsFromOss(ctx context.Context, cli *OSSClient, id ID, primary bool, p string) error {
	for _, t := range storiface.PathTypes {
		os.MkdirAll(filepath.Join(p, t.String()), 0755) // nolint

		ents, err := cli.ListSectors(t.String())
		if err != nil {
			return xerrors.Errorf("open path '%s': %w", t, err)
		}

		for _, ent := range ents {
			sid, err := storiface.ParseSectorID(ent.Name())
			if err != nil {
				return xerrors.Errorf("parse sector id %s: %w", ent.Name(), err)
			}

			if err := st.index.StorageDeclareSector(ctx, id, sid, t, primary); err != nil {
				return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", sid, t, id, err)
			}
		}
	}
	return nil
}

func (st *Local) declareSectors(ctx context.Context, p string, id ID, primary bool) error {
	for _, t := range storiface.PathTypes {
		fetchingDir := filepath.Join(p, t.String(), FetchTempSubdir)
		os.RemoveAll(fetchingDir)
		os.MkdirAll(fetchingDir, 0755)

		ents, err := ioutil.ReadDir(filepath.Join(p, t.String()))
		if err != nil {
			if os.IsNotExist(err) {
				if err := os.MkdirAll(filepath.Join(p, t.String()), 0755); err != nil { // nolint
					return xerrors.Errorf("openPath mkdir '%s': %w", filepath.Join(p, t.String()), err)
				}

				continue
			}
			return xerrors.Errorf("listing %s: %w", filepath.Join(p, t.String()), err)
		}

		for _, ent := range ents {
			if ent.Name() == FetchTempSubdir {
				continue
			}

			sid, err := storiface.ParseSectorID(ent.Name())
			if err != nil {
				return xerrors.Errorf("parse sector id %s: %w", ent.Name(), err)
			}

			if err := st.index.StorageDeclareSector(ctx, id, sid, t, primary); err != nil {
				return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", sid, t, id, err)
			}
		}
	}

	return nil
}

func (st *Local) reportHealth(ctx context.Context) {
	// randomize interval by ~10%
	interval := (HeartbeatInterval*100_000 + time.Duration(rand.Int63n(10_000))) / 100_000

	for {
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return
		}

		st.reportStorage(ctx)
	}
}

func (st *Local) reportStorage(ctx context.Context) {
	st.localLk.RLock()

	toReport := map[ID]HealthReport{}
	for id, p := range st.paths {
		stat, err := p.stat(st.localStorage)
		r := HealthReport{Stat: stat}
		if err != nil {
			r.Err = err.Error()
		}

		toReport[id] = r
	}

	st.localLk.RUnlock()

	for id, report := range toReport {
		if err := st.index.StorageReportHealth(ctx, id, report); err != nil {
			log.Warnf("error reporting storage health for %s (%+v): %+v", id, report, err)
		}
	}
}

func (st *Local) Reserve(ctx context.Context, sid storage.SectorRef, ft storiface.SectorFileType, storageIDs storiface.SectorPaths, overheadTab map[storiface.SectorFileType]int) (func(), error) {
	ssize, err := sid.ProofType.SectorSize()
	if err != nil {
		return nil, err
	}

	st.localLk.Lock()

	done := func() {}
	deferredDone := func() { done() }
	defer func() {
		st.localLk.Unlock()
		deferredDone()
	}()

	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		id := ID(storiface.PathByType(storageIDs, fileType))

		p, ok := st.paths[id]
		if !ok {
			return nil, errPathNotFound
		}

		stat, err := p.stat(st.localStorage)
		if err != nil {
			return nil, xerrors.Errorf("getting local storage stat: %w", err)
		}

		overhead := int64(overheadTab[fileType]) * int64(ssize) / storiface.FSOverheadDen

		if stat.Available < overhead {
			return nil, storiface.Err(storiface.ErrTempAllocateSpace, xerrors.Errorf("can't reserve %d bytes in '%s' (id:%s), only %d available", overhead, p.local, id, stat.Available))
		}

		p.reserved += overhead

		prevDone := done
		done = func() {
			prevDone()

			st.localLk.Lock()
			defer st.localLk.Unlock()

			p.reserved -= overhead
		}
	}

	deferredDone = func() {}
	return done, nil
}

func getCacheFilesForSectorSize(cacheDir string, ssize abi.SectorSize) []string {
	files := []string{}

	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		files = append(files, filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat"))
	case 32 << 30:
		for i := 0; i < 8; i++ {
			files = append(files, filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i)))
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			files = append(files, filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i)))
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
	return files
}

func (st *Local) checkPathIntegrity(ctx context.Context, ssize abi.SectorSize, p *path, sector storage.SectorRef, fileType storiface.SectorFileType) bool {
	spath := p.sectorPath(sector.ID, fileType)
	if int(ssize) == -1 || int(ssize) == 0 || int(ssize) == 2048 {
		log.Infof("%s: hack spt from stupid case, just ignore", spath)
		return true
	}

	sectorSize := ssize

	if p.oss {
		files := []string{spath}
		var err error

		log.Infof("check %s exists in oss start", spath)
        if fileType == storiface.FTCache {
			files = getCacheFilesForSectorSize(spath, ssize)
			err = st.checkSectorInOss(ctx, p.ossClient, p.local, storiface.SectorName(sector.ID), files, fileType, ssize, false)
		} else {
			err = st.checkSectorInOss(ctx, p.ossClient, p.local, storiface.SectorName(sector.ID), files, fileType, ssize, true)
		}
		log.Infof("check %s exists in oss end: %v", spath, err)
		if err != nil {
			log.Errorf("%s: stat [%v]", spath, err)
			return false
		}
	} else {
		fileInfo, err := os.Stat(spath)
		if nil != err {
			log.Errorf("%s: stat [%v]", spath, err)
			return false
		}

		if strings.Contains(spath, "/sealed/") {
			if int64(sectorSize) != fileInfo.Size() {
				log.Errorf("%s: %v != %v", spath, sectorSize, fileInfo.Size())
				return false
			}
		} else if strings.Contains(spath, "/unsealed/") {
			// DONT know how to check unseal size
		} else if strings.Contains(spath, "/cache/") {
			if !fileInfo.IsDir() {
				log.Errorf("%s: is not directory", spath)
				os.RemoveAll(spath)
				return false
			}
			files, err := ioutil.ReadDir(spath)
			if nil != err {
				log.Errorf("%s: readdir [%v]", spath, err)
				return false
			}
			for _, file := range files {
				if !file.Mode().IsRegular() {
					continue
				}
				fileName := spath + "/" + file.Name()
				if strings.HasSuffix(fileName, ".fp") {
					continue
				}

				fileInfo, err := os.Stat(fileName)
				if nil != err {
					log.Errorf("%s in %s: stat %v", fileName, spath, err)
					return false
				}
				if strings.Contains(fileName, "data-layer") {
					if int64(sectorSize) != fileInfo.Size() {
						log.Errorf("%s in %s: %v != %v", fileName, spath, sectorSize, fileInfo.Size())
						return false
					}
				} else if strings.Contains(fileName, "data-tree-d") {
					var treeSize int64 = int64(sectorSize)*2 - 32
					if treeSize != fileInfo.Size() {
						log.Errorf("%s in %s: %v != %v", fileName, spath, treeSize, fileInfo.Size())
						return false
					}
				}
			}
		}
	}

	return true
}

func (st *Local) AcquireSector(ctx context.Context, sid storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, pathType storiface.PathType, op storiface.AcquireMode) (storiface.SectorPaths, storiface.SectorPaths, error) {
	if existing|allocate != existing^allocate {
		return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.New("can't both find and allocate a sector")
	}

	ssize, err := sid.ProofType.SectorSize()
	if err != nil {
		return storiface.SectorPaths{}, storiface.SectorPaths{}, err
	}

	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out storiface.SectorPaths
	var storageIDs storiface.SectorPaths

	for _, fileType := range storiface.PathTypes {
		if fileType&existing == 0 {
			continue
		}

		si, err := st.index.StorageFindSector(ctx, sid.ID, fileType, ssize, false)
		if err != nil {
			log.Warnf("finding existing sector %d(t:%d) failed: %+v", sid, fileType, err)
			continue
		}

		for _, info := range si {
			p, ok := st.paths[info.ID]
			if !ok {
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				continue
			}

			spath := p.sectorPath(sid.ID, fileType)
			if !st.checkPathIntegrity(ctx, ssize, p, sid, fileType) {
				continue
			}

			storiface.SetPathByType(&out, fileType, spath)
			storiface.SetPathByType(&storageIDs, fileType, string(info.ID))

			ossInfo := storiface.SectorOSSInfo{}

			if p.oss {
				bucketName, err := p.ossClient.BucketNameByPrefix(fileType.String())
				if err != nil {
					return storiface.SectorPaths{}, storiface.SectorPaths{}, err
				}
				ossInfo.BucketName = bucketName
				ossInfo.URL = p.ossInfo.URL
				ossInfo.AccessKey = p.ossInfo.AccessKey
				ossInfo.SecretKey = p.ossInfo.SecretKey
				ossInfo.Prefix = p.ossInfo.Prefix
				ossInfo.LandedDir = p.local
				ossInfo.SectorName = storiface.SectorName(sid.ID)
			}

			storiface.SetPathExtByType(&out, fileType, p.oss, p.ossClient, ossInfo)
			storiface.SetPathExtByType(&storageIDs, fileType, p.oss, p.ossClient, ossInfo)

			existing ^= fileType
			break
		}
	}

	for _, fileType := range storiface.PathTypes {
		if fileType&allocate == 0 {
			continue
		}

		sis, err := st.index.StorageBestAlloc(ctx, fileType, ssize, pathType)
		if err != nil {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.Errorf("finding best storage for allocating : %w", err)
		}

		var best string
		var bestID ID
		var bestPath *path

		for _, si := range sis {
			p, ok := st.paths[si.ID]
			if !ok {
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				continue
			}

			if (pathType == storiface.PathSealing) && !si.CanSeal {
				continue
			}

			if (pathType == storiface.PathStorage) && !si.CanStore {
				continue
			}

			// TODO: Check free space

			best = p.sectorPath(sid.ID, fileType)
			bestID = si.ID
			bestPath = p

			break
		}

		if best == "" {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.Errorf("couldn't find a suitable path for a sector")
		}

		storiface.SetPathByType(&out, fileType, best)
		storiface.SetPathByType(&storageIDs, fileType, string(bestID))

		ossInfo := storiface.SectorOSSInfo{}

		if bestPath.oss {
			bucketName, err := bestPath.ossClient.BucketNameByPrefix(fileType.String())
			if err != nil {
				return storiface.SectorPaths{}, storiface.SectorPaths{}, err
			}
			ossInfo.BucketName = bucketName
			ossInfo.URL = bestPath.ossInfo.URL
			ossInfo.AccessKey = bestPath.ossInfo.AccessKey
			ossInfo.SecretKey = bestPath.ossInfo.SecretKey
			ossInfo.Prefix = bestPath.ossInfo.Prefix
			ossInfo.LandedDir = bestPath.local
			ossInfo.SectorName = storiface.SectorName(sid.ID)
		}

		storiface.SetPathExtByType(&storageIDs, fileType, bestPath.oss, bestPath.ossClient, ossInfo)
		storiface.SetPathExtByType(&out, fileType, bestPath.oss, bestPath.ossClient, ossInfo)

		allocate ^= fileType
	}

	return out, storageIDs, nil
}

func (st *Local) Local(ctx context.Context) ([]StoragePath, error) {
	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out []StoragePath
	for id, p := range st.paths {
		if p.local == "" {
			continue
		}

		si, err := st.index.StorageInfo(ctx, id)
		if err != nil {
			return nil, xerrors.Errorf("get storage info for %s: %w", id, err)
		}

		out = append(out, StoragePath{
			ID:        id,
			Weight:    si.Weight,
			LocalPath: p.local,
			CanSeal:   si.CanSeal,
			CanStore:  si.CanStore,
		})
	}

	return out, nil
}

func (st *Local) Remove(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType, force bool) error {
	if bits.OnesCount(uint(typ)) != 1 {
		return xerrors.New("delete expects one file type")
	}

	si, err := st.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	if len(si) == 0 && !force {
		return xerrors.Errorf("can't delete sector %v(%d), not found", sid, typ)
	}

	for _, info := range si {
		if err := st.removeSector(ctx, sid, typ, info.ID); err != nil {
			return err
		}
	}

	return nil
}

func (st *Local) MoveCache(ctx context.Context, sid storage.SectorRef, typ storiface.SectorFileType, force bool) error {
	si, err := st.index.StorageFindSector(ctx, sid.ID, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	if len(si) == 0 && !force {
		return xerrors.Errorf("can't delete sector %v(%d), not found", sid, typ)
	}

	for _, info := range si {
		if err := st.moveCacheSector(ctx, sid.ID, typ, info.ID); err != nil {
			return err
		}
	}

	return nil
}

func (st *Local) moveCacheSector(ctx context.Context, sid abi.SectorID, fileType storiface.SectorFileType, storage ID) error {
	p, ok := st.paths[storage]
	if !ok {
		return nil
	}

	if p.local == "" { // TODO: can that even be the case?
		return nil
	}

	if err := st.index.StorageDropSector(ctx, storage, sid, fileType); err != nil {
		return xerrors.Errorf("dropping sector from index: %w", err)
	}

	spath := p.sectorPath(sid, fileType)
	log.Debugf("go moving cache sector (%v) from %s", sid, spath)
	if err := moveCacheEx(spath); err != nil {
		log.Warnf("move cache sector (%v) %s to hdd error: %s", sid, spath, err)
	}

	if err := st.index.StorageDeclareSector(ctx, storage, sid, fileType, true); err != nil {
		return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", sid, fileType, storage, err)
	}

	return nil
}

func (r *Remote) MoveCache(ctx context.Context, sid storage.SectorRef, typ storiface.SectorFileType, force bool) error {
	si, err := r.index.StorageFindSector(ctx, sid.ID, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	for _, info := range si {
		for _, url := range info.URLs {
			if err := r.moveCacheRemote(ctx, url); err != nil {
				return xerrors.Errorf("fail move cache %s: %+v", url, err)
			}
		}
	}

	return nil
}

func (r *Remote) moveCacheRemote(ctx context.Context, url string) error {
	log.Infof("MOVE %s", url)

	req, err := http.NewRequest("MOVE", url, nil)
	if err != nil {
		return xerrors.Errorf("request: %w", err)
	}
	req.Header = r.auth
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return xerrors.Errorf("do request: %w", err)
	}
	defer resp.Body.Close() // nolint

	if resp.StatusCode != 200 {
		return xerrors.Errorf("non-200 code: %d", resp.StatusCode)
	}

	return nil
}

func isLink(filename string) bool {
	fileInfo, err := os.Lstat(filename)
	if err != nil {
		return false
	}

	if (os.ModeSymlink & fileInfo.Mode()) > 0 {
		return true
	}

	return false
}

func envMove(from, to string) error {
	log.Debugw("start to check the file is linked", "from:", from, "to:", to)
	if isLink(from) {
		log.Debugw("already linked", "from:", from, "to:", to)
		return nil
	}
	log.Debugw("check the file is not linked,and then move to hdd", "from:", from, "to:", to)

	fp := from + ".fp"
	if !isExists(fp) {
		//create fp file
		f, err := os.Create(fp)
		defer f.Close()
		if err != nil {
			log.Errorf("creat fp file error %w", err)
		} else {
			f.Write([]byte(to))
			log.Debugw("write fp file", "content", to)
		}
	} else {
		//read fp
		f, err := os.OpenFile(fp, os.O_RDWR, 0600)
		defer f.Close()
		if err != nil {
			log.Errorf("open fp file error %w", err)
		} else {
			c, _ := ioutil.ReadAll(f)
			oldto := string(c)
			log.Infow("read fp file:", "content", oldto)
			if isExists(from) {
				f.Truncate(0)
				f.WriteAt([]byte(to), 0)
				log.Debugw("write new target file", "to", to)

				if err := os.Remove(oldto); err != nil {
					log.Errorw("remove old target file failed", "oldto:", oldto)
				} else {
					log.Debugw("remove old target file", "oldto", oldto)
				}
			} else {
				if isExists(oldto) {
					var errOut bytes.Buffer
					cmdln := exec.Command("/usr/bin/env", "ln", "-s", oldto, from)
					cmdln.Stderr = &errOut
					if err := cmdln.Run(); err != nil {
						return xerrors.Errorf("exec re-ln (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
					}
					log.Debugw("re-link sector data", "from:", from, "oldto:", oldto)

					if err := os.Remove(fp); err != nil {
						log.Errorw("remove fp file failed", "fp:", fp)
					}
					return nil
				}
				return xerrors.Errorf("exec re-ln failed")
			}
		}
	}

	var errOut bytes.Buffer
	cmd := exec.Command("/usr/bin/env", "mv", "-f", from, to)
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("exec cp (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}
	log.Debugw("move sector data", "from:", from, "to:", to)

	//make soft link
	cmdln := exec.Command("/usr/bin/env", "ln", "-s", to, from)
	cmdln.Stderr = &errOut
	if err := cmdln.Run(); err != nil {
		return xerrors.Errorf("exec ln (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}
	log.Debugw("link sector data", "from:", from, "to:", to)

	if err := os.Remove(fp); err != nil {
		log.Debugw("remove fp file failed", "fp:", fp)
	}

	return nil
}

func sortBySize(pl []os.FileInfo) []os.FileInfo {
	sort.Slice(pl, func(i, j int) bool {
		flag := false
		if pl[i].Size() > pl[j].Size() {
			flag = true
		}
		return flag
	})
	return pl
}

// che_path = local_path + sector_name
func moveCacheEx(che_path string) error {
	log.Debugw("move cache to hdd starting", "path", che_path)
	var mv_files []string
	var hd_paths []string
	var sect_name string
	//////////////////////////////////////////////
	spl := strings.Split(che_path, string(os.PathSeparator))
	sect_name = spl[len(spl)-1]

	if sect_dir, err := ioutil.ReadDir(che_path); err == nil {
		sect_dir_sort := sortBySize(sect_dir)
		for _, fi := range sect_dir_sort {
			if fi.IsDir() {
				continue
			}

			fileAdded := false
			fileName := strings.ReplaceAll(fi.Name(), ".fp", "")

			for _, file := range mv_files {
				if file == fileName {
					fileAdded = true
					break
				}
			}

			if !fileAdded {
				mv_files = append(mv_files, fileName)
			}
		}
	}
	log.Debugw("all files to move", "files", mv_files)

	env := os.Getenv("LOTUS_CACHE_HDD")
	if env == "" {
		return xerrors.Errorf("There is no environment variable: LOTUS_CACHE_HDD")
	}
	hdd := strings.Split(env, ";")
	for index := 0; index < len(hdd); index++ {
		fs := syscall.Statfs_t{}
		if err := syscall.Statfs(hdd[index], &fs); err != nil {
			log.Errorw("Get system stats error", "path", hdd[index], "error", err)
			continue
		}

		if fs.Bfree*uint64(fs.Bsize) < 1024*1024*64 {
			log.Errorw("no enough space", "path", che_path, "disk free", fs.Bfree*uint64(fs.Bsize))
			continue
		}
		path := hdd[index] + string(os.PathSeparator) + sect_name
		if err := os.MkdirAll(path, 0755); err != nil {
			log.Warnf("Create hdd cache(%s) err: %s", path, err)
			continue
		}
		hd_paths = append(hd_paths, path)
	}
	log.Debugw("all hdd for cache", "hdd_path", hd_paths)

	if len(mv_files) == 0 || len(hd_paths) == 0 {
		log.Warnw("no enough to move cache to hdd.")
		return xerrors.Errorf("no enough to move cache to hdd.")
	}
	//////////////////////////////////////////////
	jobs := make(chan string, len(mv_files))
	wait := &sync.WaitGroup{}

	wait.Add(1)
	go func(fis []string, ch chan string, wt *sync.WaitGroup) {
		defer wt.Done()
		for i := 0; i < len(fis); i++ {
			ch <- che_path + string(os.PathSeparator) + fis[i]
		}
		close(ch)
	}(mv_files, jobs, wait)

	for i := 0; i < len(hd_paths); i++ {
		wait.Add(1)
		go func(to_path string, ch chan string, wt *sync.WaitGroup) {
			defer wt.Done()
			for {
				from, ok := <-ch
				if !ok {
					log.Debugw("there is no more move jobs", "to-hdd", to_path)
					break
				}
				to := to_path + string(os.PathSeparator) + filepath.Base(from)
				if err := envMove(from, to); err != nil {
					log.Errorw("copy file to hdd err", "from", from, "to", to, "error", err)
					continue
				}
			}
		}(hd_paths[i], jobs, wait)
	}

	wait.Wait()
	log.Debugw("move cache to hdd over.")
	return nil
	//////////////////////////////////////////////
}

func (st *Local) RemoveCopies(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType) error {
	if bits.OnesCount(uint(typ)) != 1 {
		return xerrors.New("delete expects one file type")
	}

	si, err := st.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	var hasPrimary bool
	for _, info := range si {
		if info.Primary {
			hasPrimary = true
			break
		}
	}

	if !hasPrimary {
		log.Warnf("RemoveCopies: no primary copies of sector %v (%s), not removing anything", sid, typ)
		return nil
	}

	for _, info := range si {
		if info.Primary {
			continue
		}

		if err := st.removeSector(ctx, sid, typ, info.ID); err != nil {
			return err
		}
	}

	return nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func isExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func isDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func isFile(path string) bool {
	return !isDir(path)
}

func (st *Local) removeSector(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType, storage ID) error {
	p, ok := st.paths[storage]
	if !ok {
		return nil
	}

	if p.local == "" { // TODO: can that even be the case?
		return nil
	}

	if err := st.index.StorageDropSector(ctx, storage, sid, typ); err != nil {
		return xerrors.Errorf("dropping sector from index: %w", err)
	}

	spath := p.sectorPath(sid, typ)
	log.Debugf("remove %s", spath)

	if err := os.RemoveAll(spath); err != nil {
		log.Errorf("removing sector (%v) from %s: %+v", sid, spath, err)
	}

	st.reportStorage(ctx) // report freed space

	if typ == storiface.FTCache {
		env_hdd := os.Getenv("LOTUS_CACHE_HDD")
		if env_hdd == "" {
			return nil
		}

		hdd := strings.Split(env_hdd, ";")
		for index := 0; index < len(hdd); index++ {
			cache_hdd := hdd[index]
			cache_hdd, _ = homedir.Expand(cache_hdd)
			spl := strings.Split(spath, string(os.PathSeparator))
			cache_hdd = cache_hdd + string(os.PathSeparator) + spl[len(spl)-1]

			if flag, err := pathExists(cache_hdd); !flag {
				log.Warnf("check hdd cache sector (%v) not in %s: %+v", sid, cache_hdd, err)
				continue
			}

			if err := os.RemoveAll(cache_hdd); err != nil {
				log.Errorf("removing hdd cache sector (%v) from %s: %+v", sid, cache_hdd, err)
				continue
			}
			log.Debugf("remove hdd cache %s", cache_hdd)
		}
	}

	return nil
}

func (st *Local) MoveStorage(ctx context.Context, s storage.SectorRef, types storiface.SectorFileType) error {
	dest, destIds, err := st.AcquireSector(ctx, s, storiface.FTNone, types, storiface.PathStorage, storiface.AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire dest storage: %w", err)
	}

	src, srcIds, err := st.AcquireSector(ctx, s, types, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire src storage: %w", err)
	}

	for _, fileType := range storiface.PathTypes {
		if fileType&types == 0 {
			continue
		}

		sst, err := st.index.StorageInfo(ctx, ID(storiface.PathByType(srcIds, fileType)))
		if err != nil {
			return xerrors.Errorf("failed to get source storage info: %w", err)
		}

		dst, err := st.index.StorageInfo(ctx, ID(storiface.PathByType(destIds, fileType)))
		if err != nil {
			return xerrors.Errorf("failed to get source storage info: %w", err)
		}

		if !dst.Oss {
			if sst.ID == dst.ID {
				log.Infof("not moving %v(%d / %v); src and dest are the same", s, fileType, src)
				continue
			}
			if sst.CanStore {
				log.Debugf("not moving %v(%d / %v); source supports storage", s, fileType, src)
				continue
			}
		}

		log.Infof("moving %v(%d) to storage: %s(se:%t; st:%t) -> %s(se:%t; st:%t)", s, fileType, sst.ID, sst.CanSeal, sst.CanStore, dst.ID, dst.CanSeal, dst.CanStore)

		if err := st.index.StorageDropSector(ctx, ID(storiface.PathByType(srcIds, fileType)), s.ID, fileType); err != nil {
			return xerrors.Errorf("dropping source sector from index: %w", err)
		}

		ossUploaded := false
		var moveErr error

		if dst.Oss {
			// If file is not exist, do not upload
			// That means: it's already uploaded at sealing side
			_, err := os.Stat(storiface.PathByType(src, fileType))
			if err == nil {
				for _, p := range st.paths {
					if p.oss && p.ossInfo.Equal(&dst.OssInfo) && dst.CanStore {
						moveErr = upload(storiface.PathByType(src, fileType), fileType.String(), storiface.SectorName(s.ID), p.ossClient, true)
						if moveErr != nil {
							log.Errorf("cannot upload %v to oss storage %v", storiface.SectorName(s.ID), moveErr)
						}
						ossUploaded = true
						break
					}
				}
				if !ossUploaded {
					moveErr = xerrors.Errorf("cannot find suitable uploader")
				}
			}
		} else {
			if err := move(storiface.PathByType(src, fileType), storiface.PathByType(dest, fileType)); err != nil {
				// TODO: attempt some recovery (check if src is still there, re-declare)
				moveErr = xerrors.Errorf("moving sector %v(%d): %w", s, fileType, err)
			}
		}

		if err := st.index.StorageDeclareSector(ctx, ID(storiface.PathByType(destIds, fileType)), s.ID, fileType, true); err != nil {
			return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", s, fileType, ID(storiface.PathByType(destIds, fileType)), err)
		}

		if moveErr != nil {
			return moveErr
		}
	}

	st.reportStorage(ctx) // report space use changes

	return nil
}

var errPathNotFound = xerrors.Errorf("fsstat: path not found")

func (st *Local) FsStat(ctx context.Context, id ID) (fsutil.FsStat, error) {
	st.localLk.RLock()
	defer st.localLk.RUnlock()

	p, ok := st.paths[id]
	if !ok {
		return fsutil.FsStat{}, errPathNotFound
	}

	return p.stat(st.localStorage)
}

func (st *Local) CheckSectorInOss(ctx context.Context, p storiface.SectorPath, files []string, ft storiface.SectorFileType, ssize abi.SectorSize, checkSize bool) error {
	return st.checkSectorInOss(ctx, p.Private.(*OSSClient), p.OssInfo.LandedDir, p.OssInfo.SectorName, files, ft, ssize, checkSize)
}

func (st *Local) checkSectorInOss(ctx context.Context, ossCli *OSSClient, landedDir string, sectorName string, files []string, ft storiface.SectorFileType, ssize abi.SectorSize, checkSize bool) error {
	prefix := ft.String()

	if 0 < len(files) {
		for _, file := range files {
			objName := strings.TrimPrefix(file, landedDir)
			objName = strings.TrimPrefix(objName, "/")
			objName = strings.TrimPrefix(objName, prefix)
			size, err := ossCli.HeadObject(prefix, objName)
			if err != nil {
				return xerrors.Errorf("fail to query object %v in oss (%v)", objName, err)
			}
			if !checkSize && size <= 0 {
				return xerrors.Errorf("%s's size is invalid (%v)", objName, size)
			}
		}
	} else {
		objName := sectorName
		size, err := ossCli.HeadObject(prefix, objName)
		if err != nil {
			return xerrors.Errorf("fail to query object %v in oss (%v)", objName, err)
		}
		if !checkSize && size <= 0 {
			return xerrors.Errorf("%s's size is invalid (%v)", objName, size)
		}
		if checkSize && size != int64(ssize) {
			return xerrors.Errorf("%s's size is not equal (%v != %v)", objName, size, ssize)
		}
	}

	return nil
}

var _ Store = &Local{}
