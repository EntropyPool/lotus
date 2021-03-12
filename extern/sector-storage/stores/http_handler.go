package stores

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/extern/sector-storage/tarutil"

	"github.com/filecoin-project/specs-storage/storage"
)

var log = logging.Logger("stores")

type FetchHandler struct {
	*Local
}

func (handler *FetchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) { // /remote/
	mux := mux.NewRouter()

	mux.HandleFunc("/remote/stat/{id}", handler.remoteStatFs).Methods("GET")
	mux.HandleFunc("/remote/{type}/{id}", handler.remoteGetSector).Methods("GET")
	mux.HandleFunc("/remote/{type}/{id}", handler.remoteDeleteSector).Methods("DELETE")
	mux.HandleFunc("/remote/{type}/{id}", handler.remoteMoveCacheSector).Methods("MOVE")

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

	id, err := storiface.ParseSectorID(vars["id"])
	if err != nil {
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}

	ft, err := ftFromString(vars["type"])
	if err != nil {
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}

	r.ParseForm()

	// The caller has a lock on this sector already, no need to get one here

	// passing 0 spt because we don't allocate anything
	si := storage.SectorRef{
		ID:        id,
		ProofType: 0,
	}

	paths, _, err := handler.Local.AcquireSector(r.Context(), si, ft, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
	if err != nil {
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}

	// TODO: reserve local storage here

	path := storiface.PathByType(paths, ft)
	if path == "" {
		log.Error("acquired path was empty")
		w.WriteHeader(500)
		return
	}

	stat, err := os.Stat(path)
	if err != nil {
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}

	oss := false
	if _, ok := r.Form["oss"]; ok {
		oss, err = strconv.ParseBool(r.Form["oss"][0])
		if err != nil {
			log.Errorf("%+v", err)
			w.WriteHeader(500)
			return
		}
	}

	if oss {
		ossCli, err := NewOSSClientWithSingleBucket(StorageOSSInfo{
			URL:        r.Form["oss_url"][0],
			AccessKey:  r.Form["oss_access_key"][0],
			SecretKey:  r.Form["oss_secret_key"][0],
			BucketName: r.Form["oss_bucket_name"][0],
			Prefix:     r.Form["oss_prefix"][0],
			CanWrite:   true,
		})

		if err != nil {
			log.Errorf("%+v", err)
			w.WriteHeader(500)
			return
		}

		err = upload(path, vars["type"], vars["id"], ossCli, false)
		if err != nil {
			log.Errorf("%+v", err)
			w.WriteHeader(500)
			return
		}

		w.WriteHeader(200)
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
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
	if _, err := io.Copy(w, rd); err != nil { // TODO: default 32k buf may be too small
		log.Errorf("%+v", err)
		return
	}
}

func (handler *FetchHandler) remoteDeleteSector(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	log.Infof("SERVE DELETE %s [%v]", r.URL, vars)

	id, err := storiface.ParseSectorID(vars["id"])
	if err != nil {
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}

	ft, err := ftFromString(vars["type"])
	if err != nil {
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}

	if err := handler.Remove(r.Context(), id, ft, false); err != nil {
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}
}

func (handler *FetchHandler) remoteMoveCacheSector(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	log.Infof("SERVE MOVE %s [%v]", r.URL, vars)

	id, err := storiface.ParseSectorID(vars["id"])
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

	si := storage.SectorRef{
		ID:        id,
		ProofType: 0,
	}

	if err := handler.MoveCache(r.Context(), si, ft, false); err != nil {
		log.Errorf("%+v", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
	log.Infof("SERVE MOVE over %s", r.URL)
	return
}

func ftFromString(t string) (storiface.SectorFileType, error) {
	switch t {
	case storiface.FTUnsealed.String():
		return storiface.FTUnsealed, nil
	case storiface.FTSealed.String():
		return storiface.FTSealed, nil
	case storiface.FTCache.String():
		return storiface.FTCache, nil
	default:
		return 0, xerrors.Errorf("unknown sector file type: '%s'", t)
	}
}
