package stores

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/mitchellh/go-homedir"
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

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
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
	ID     ID
	Weight uint64 // 0 = readonly

	CanSeal  bool
	CanStore bool
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

var PathTypes = []SectorFileType{FTUnsealed, FTSealed, FTCache}

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
	reservations map[abi.SectorID]SectorFileType
}

func (p *path) stat(ls LocalStorage) (fsutil.FsStat, error) {
	stat, err := ls.Stat(p.local)
	if err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("stat %s: %w", p.local, err)
	}

	stat.Reserved = p.reserved

	for id, ft := range p.reservations {
		for _, fileType := range PathTypes {
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

func (p *path) sectorPath(sid abi.SectorID, fileType SectorFileType) string {
	return filepath.Join(p.local, fileType.String(), SectorName(sid))
}

func NewLocal(ctx context.Context, ls LocalStorage, index SectorIndex, urls []string) (*Local, error) {
	l := &Local{
		localStorage: ls,
		index:        index,
		urls:         urls,

		paths: map[ID]*path{},
	}
	return l, l.open(ctx)
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
		reservations: map[abi.SectorID]SectorFileType{},
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
	}, fst)
	if err != nil {
		return xerrors.Errorf("declaring storage in index: %w", err)
	}

	for _, t := range PathTypes {
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

			sid, err := ParseSectorID(ent.Name())
			if err != nil {
				return xerrors.Errorf("parse sector id %s: %w", ent.Name(), err)
			}

			if err := st.index.StorageDeclareSector(ctx, meta.ID, sid, t, meta.CanStore); err != nil {
				return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", sid, t, meta.ID, err)
			}
		}
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

func (st *Local) reportHealth(ctx context.Context) {
	// randomize interval by ~10%
	interval := (HeartbeatInterval*100_000 + time.Duration(rand.Int63n(10_000))) / 100_000

	for {
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return
		}

		st.localLk.RLock()

		toReport := map[ID]HealthReport{}
		for id, p := range st.paths {
			stat, err := p.stat(st.localStorage)

			toReport[id] = HealthReport{
				Stat: stat,
				Err:  err,
			}
		}

		st.localLk.RUnlock()

		for id, report := range toReport {
			if err := st.index.StorageReportHealth(ctx, id, report); err != nil {
				log.Warnf("error reporting storage health for %s: %+v", id, report)
			}
		}
	}
}

func (st *Local) Reserve(ctx context.Context, sid abi.SectorID, spt abi.RegisteredSealProof, ft SectorFileType, storageIDs SectorPaths, overheadTab map[SectorFileType]int) (func(), error) {
	ssize, err := spt.SectorSize()
	if err != nil {
		return nil, xerrors.Errorf("getting sector size: %w", err)
	}

	st.localLk.Lock()

	done := func() {}
	deferredDone := func() { done() }
	defer func() {
		st.localLk.Unlock()
		deferredDone()
	}()

	for _, fileType := range PathTypes {
		if fileType&ft == 0 {
			continue
		}

		id := ID(PathByType(storageIDs, fileType))

		p, ok := st.paths[id]
		if !ok {
			return nil, errPathNotFound
		}

		stat, err := p.stat(st.localStorage)
		if err != nil {
			return nil, xerrors.Errorf("getting local storage stat: %w", err)
		}

		overhead := int64(overheadTab[fileType]) * int64(ssize) / FSOverheadDen

		if stat.Available < overhead {
			return nil, xerrors.Errorf("can't reserve %d bytes in '%s' (id:%s), only %d available", overhead, p.local, id, stat.Available)
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

func (st *Local) AcquireSector(ctx context.Context, sid abi.SectorID, spt abi.RegisteredSealProof, existing SectorFileType, allocate SectorFileType, pathType PathType, op AcquireMode) (SectorPaths, SectorPaths, error) {
	if existing|allocate != existing^allocate {
		return SectorPaths{}, SectorPaths{}, xerrors.New("can't both find and allocate a sector")
	}

	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out SectorPaths
	var storageIDs SectorPaths

	for _, fileType := range PathTypes {
		if fileType&existing == 0 {
			continue
		}

		si, err := st.index.StorageFindSector(ctx, sid, fileType, spt, false)
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

			spath := p.sectorPath(sid, fileType)
			SetPathByType(&out, fileType, spath)
			SetPathByType(&storageIDs, fileType, string(info.ID))

			existing ^= fileType
			break
		}
	}

	for _, fileType := range PathTypes {
		if fileType&allocate == 0 {
			continue
		}

		sis, err := st.index.StorageBestAlloc(ctx, fileType, spt, pathType)
		if err != nil {
			return SectorPaths{}, SectorPaths{}, xerrors.Errorf("finding best storage for allocating : %w", err)
		}

		var best string
		var bestID ID

		for _, si := range sis {
			p, ok := st.paths[si.ID]
			if !ok {
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				continue
			}

			if (pathType == PathSealing) && !si.CanSeal {
				continue
			}

			if (pathType == PathStorage) && !si.CanStore {
				continue
			}

			// TODO: Check free space

			best = p.sectorPath(sid, fileType)
			bestID = si.ID
			break
		}

		if best == "" {
			return SectorPaths{}, SectorPaths{}, xerrors.Errorf("couldn't find a suitable path for a sector")
		}

		SetPathByType(&out, fileType, best)
		SetPathByType(&storageIDs, fileType, string(bestID))
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

func (st *Local) Remove(ctx context.Context, sid abi.SectorID, typ SectorFileType, force bool) error {
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

func (st *Local) MoveCache(ctx context.Context, sid abi.SectorID, typ SectorFileType, force bool) error {

	si, err := st.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	if len(si) == 0 && !force {
		return xerrors.Errorf("can't delete sector %v(%d), not found", sid, typ)
	}

	for _, info := range si {
		if err := st.moveCacheSector(ctx, sid, typ, info.ID); err != nil {
			return err
		}
	}

	return nil
}

func (st *Local) moveCacheSector(ctx context.Context, sid abi.SectorID, typ SectorFileType, storage ID) error {
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
	log.Infof("go moving cache sector (%v) from %s", sid, spath)
	if err := move_cache_ex(spath); err != nil {
		return xerrors.Errorf("move cache sector (%v) %s to hdd error: %w", sid, spath, err)
	}

	return nil
}

func (r *Remote) MoveCache(ctx context.Context, sid abi.SectorID, typ SectorFileType, force bool) error {
	si, err := r.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	err_url := ""
	for _, info := range si {
		for _, url := range info.URLs {
			if err := r.moveCacheRemote(ctx, url); err != nil {
				log.Errorf("move cache %s: %+v", url, err)
				err_url += err_url + url
				continue
			}
		}
	}
	if err_url != "" {
		return xerrors.Errorf("move cache error: %s", err_url)
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

func envmove(from, to string) error {

	var errOut bytes.Buffer
	cmd := exec.Command("/usr/bin/env", "mv", "-f", from, to)
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("exec cp (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}
	log.Debugw("copy sector data", "from:", from, "to:", to)

	//make soft link
	cmdln := exec.Command("/usr/bin/env", "ln", "-s", to, from)
	cmdln.Stderr = &errOut
	if err := cmdln.Run(); err != nil {
		return xerrors.Errorf("exec ln (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}
	log.Debugw("link sector data", "from:", from, "to:", to)

	return nil
}

func sort_by_size(pl []os.FileInfo) []os.FileInfo {
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
func move_cache_ex(che_path string) error {
	log.Infow("move cache to hdd starting", "path", che_path)
	var mv_files []string
	var hd_paths []string
	var sect_name string
	//////////////////////////////////////////////
	spl := strings.Split(che_path, string(os.PathSeparator))
	sect_name = spl[len(spl)-1]

	if sect_dir, err := ioutil.ReadDir(che_path); err == nil {
		sect_dir_sort := sort_by_size(sect_dir)
		for _, fi := range sect_dir_sort {
			if fi.IsDir() {
				continue
			}
			mv_files = append(mv_files, fi.Name())
		}
	}
	log.Infow("all files to move", "files", mv_files)

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
		//1024*1024*1024*64
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
	log.Infow("all hdd for cache", "hdd_path", hd_paths)

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
					log.Infow("there is no more move jobs", "to-hdd", to_path)
					break
				}
				to := to_path + string(os.PathSeparator) + filepath.Base(from)
				if err := envmove(from, to); err != nil {
					log.Errorw("copy file to hdd err", "from", from, "to", to, "error", err)
					continue
				}
			}
		}(hd_paths[i], jobs, wait)
	}

	wait.Wait()
	log.Infow("move cache to hdd over.")
	return nil
	//////////////////////////////////////////////
}

func (st *Local) RemoveCopies(ctx context.Context, sid abi.SectorID, typ SectorFileType) error {
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

func (st *Local) removeSector(ctx context.Context, sid abi.SectorID, typ SectorFileType, storage ID) error {
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
	log.Infof("remove %s", spath)

	if err := os.RemoveAll(spath); err != nil {
		log.Errorf("removing sector (%v) from %s: %+v", sid, spath, err)
	}

	if typ == FTCache {
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
				log.Infof("check hdd cache sector (%v) not in %s: %+v", sid, cache_hdd, err)
				continue
			}

			if err := os.RemoveAll(cache_hdd); err != nil {
				log.Errorf("removing hdd cache sector (%v) from %s: %+v", sid, cache_hdd, err)
				continue
			}
			log.Infof("remove hdd cache %s", cache_hdd)
		}
	}

	return nil
}

func (st *Local) MoveStorage(ctx context.Context, s abi.SectorID, spt abi.RegisteredSealProof, types SectorFileType) error {
	dest, destIds, err := st.AcquireSector(ctx, s, spt, FTNone, types, PathStorage, AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire dest storage: %w", err)
	}

	src, srcIds, err := st.AcquireSector(ctx, s, spt, types, FTNone, PathStorage, AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire src storage: %w", err)
	}

	for _, fileType := range PathTypes {
		if fileType&types == 0 {
			continue
		}

		sst, err := st.index.StorageInfo(ctx, ID(PathByType(srcIds, fileType)))
		if err != nil {
			return xerrors.Errorf("failed to get source storage info: %w", err)
		}

		dst, err := st.index.StorageInfo(ctx, ID(PathByType(destIds, fileType)))
		if err != nil {
			return xerrors.Errorf("failed to get source storage info: %w", err)
		}

		if sst.ID == dst.ID {
			log.Debugf("not moving %v(%d); src and dest are the same", s, fileType)
			continue
		}

		if sst.CanStore {
			log.Debugf("not moving %v(%d); source supports storage", s, fileType)
			continue
		}

		log.Debugf("moving %v(%d) to storage: %s(se:%t; st:%t) -> %s(se:%t; st:%t)", s, fileType, sst.ID, sst.CanSeal, sst.CanStore, dst.ID, dst.CanSeal, dst.CanStore)

		if err := st.index.StorageDropSector(ctx, ID(PathByType(srcIds, fileType)), s, fileType); err != nil {
			return xerrors.Errorf("dropping source sector from index: %w", err)
		}

		if err := move(PathByType(src, fileType), PathByType(dest, fileType)); err != nil {
			// TODO: attempt some recovery (check if src is still there, re-declare)
			return xerrors.Errorf("moving sector %v(%d): %w", s, fileType, err)
		}

		if err := st.index.StorageDeclareSector(ctx, ID(PathByType(destIds, fileType)), s, fileType, true); err != nil {
			return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", s, fileType, ID(PathByType(destIds, fileType)), err)
		}
	}

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

var _ Store = &Local{}
