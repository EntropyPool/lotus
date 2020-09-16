package stores

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/extern/sector-storage/tarutil"
)

var log = logging.Logger("stores")

type FetchHandler struct {
	*Local
}

func (handler *FetchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) { // /remote/
	mux := mux.NewRouter()

	mux.HandleFunc("/remote/stat/{id}", handler.remoteStatFs).Methods("GET")
	mux.HandleFunc("/remote/{type}/{id}", handler.remoteGetSector).Methods("GET")
	mux.HandleFunc("/remote/{type}/{id}", handler.remoteListSector).Methods("LIST")
	mux.HandleFunc("/remote/{type}/{id}/{file}", handler.remoteGetFile).Methods("GET")
	mux.HandleFunc("/remote/{type}/{id}", handler.remoteDeleteSector).Methods("DELETE")

	mux.ServeHTTP(w, r)
}

func (handler *FetchHandler) remoteStatFs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := ID(vars["id"])

	st, err := handler.Local.FsStat(r.Context(), id)
	switch err {
	case errPathNotFound:
		w.WriteHeader(404)
		return
	case nil:
		break
	default:
		w.WriteHeader(500)
		log.Errorf("%+v", err)
		return
	}

	if err := json.NewEncoder(w).Encode(&st); err != nil {
		log.Warnf("error writing stat response: %+v", err)
	}
}

func (handler *FetchHandler) remoteGetSector(w http.ResponseWriter, r *http.Request) {
	log.Infof("SERVE GET %s", r.URL)
	vars := mux.Vars(r)

	id, err := ParseSectorID(vars["id"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	ft, err := ftFromString(vars["type"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	// The caller has a lock on this sector already, no need to get one here

	// passing 0 spt because we don't allocate anything
	paths, _, err := handler.Local.AcquireSector(r.Context(), id, 0, ft, FTNone, PathStorage, AcquireMove)
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	// TODO: reserve local storage here

	path := PathByType(paths, ft)
	if path == "" {
		log.Error("acquired path was empty")
		w.WriteHeader(500)
		return
	}

	stat, err := os.Stat(path)
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	var rd io.Reader
	if stat.IsDir() {
		rd, err = tarutil.TarDirectory(path)
		w.Header().Set("Content-Type", "application/x-tar")
	} else {
		rd, err = os.OpenFile(path, os.O_RDONLY, 0644) // nolint
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
	if _, err := io.Copy(w, rd); err != nil { // TODO: default 32k buf may be too small
		log.Error("%+v", err)
		return
	}
}

func (handler *FetchHandler) remoteListSector(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	for k, v := range vars {
		log.Infow("SERVE LIST VARS", "Key", k, "Val", v)
	}

	id, err := ParseSectorID(vars["id"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	ft, err := ftFromString(vars["type"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	paths, _, err := handler.Local.AcquireSector(r.Context(), id, 0, ft, FTNone, PathStorage, AcquireMove)
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	// TODO: reserve local storage here
	path := PathByType(paths, ft)
	if path == "" {
		log.Error("acquired path was empty")
		w.WriteHeader(500)
		return
	}

	stat, err := os.Stat(path)
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	files := ""
	if stat.IsDir() {
		if sect_dir, err := ioutil.ReadDir(path); err == nil {
			sort.Slice(sect_dir, func(i, j int) bool {
				return sect_dir[i].Size() > sect_dir[j].Size()
			})

			for _, fi := range sect_dir {
				if fi.IsDir() {
					continue
				}
				if files == "" {
					files = fi.Name()
				} else {
					files = files + ";" + fi.Name()
				}
			}
		}
	}
	w.Header().Set("Files-List", files)
	log.Infow("SERVE LIST ", r.URL, files)
	w.WriteHeader(200)
	return
}

func (handler *FetchHandler) remoteGetFile(w http.ResponseWriter, r *http.Request) {
	log.Infof("SERVE GET FILE %s", r.URL)
	vars := mux.Vars(r)
	log.Infow("SERVE GET FILE", "vars", vars)

	id, err := ParseSectorID(vars["id"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	ft, err := ftFromString(vars["type"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	// The caller has a lock on this sector already, no need to get one here

	// passing 0 spt because we don't allocate anything
	paths, _, err := handler.Local.AcquireSector(r.Context(), id, 0, ft, FTNone, PathStorage, AcquireMove)
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	// TODO: reserve local storage here

	path := PathByType(paths, ft)
	if path == "" {
		log.Error("acquired path was empty")
		w.WriteHeader(500)
		return
	}

	target := path + string(os.PathSeparator) + vars["file"]
	log.Infow("SERVE GET FILE", "target", target)
	if _, err := os.Stat(target); err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	var rd io.Reader
	rd, err = os.OpenFile(target, os.O_RDONLY, 0644) // nolint
	w.Header().Set("Content-Type", "application/octet-stream")
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
	if _, err := io.Copy(w, rd); err != nil { // TODO: default 32k buf may be too small
		log.Error("%+v", err)
		return
	}

}

func (handler *FetchHandler) remoteDeleteSector(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	log.Infof("SERVE DELETE %s [%v]", r.URL, vars)

	id, err := ParseSectorID(vars["id"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	ft, err := ftFromString(vars["type"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	if err := handler.Remove(r.Context(), id, ft, false); err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}
}

func ftFromString(t string) (SectorFileType, error) {
	switch t {
	case FTUnsealed.String():
		return FTUnsealed, nil
	case FTSealed.String():
		return FTSealed, nil
	case FTCache.String():
		return FTCache, nil
	default:
		return 0, xerrors.Errorf("unknown sector file type: '%s'", t)
	}
}
