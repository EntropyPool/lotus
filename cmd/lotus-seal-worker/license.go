package main

import (
	machspec "github.com/EntropyPool/machine-spec"
	lic "github.com/filecoin-project/lotus/fbc-license"
	"os"
	"time"
)

var shouldStop = 0

func startLicenseClient(username string, password string) *lic.LicenseClient {
	spec := machspec.NewMachineSpec()
	spec.PrepareLowLevel()
	sn := spec.SN()

	cli := lic.NewLicenseClient(lic.LicenseConfig{
		ClientUser:     username,
		ClientUserPass: password,
		NetworkType:    "filecoin",
		ClientSn:       sn,
		LicenseServer:  "license.npool.top",
		Scheme:         "https",
	})
	go cli.Run()

	return cli
}

func checkLicense(cli *lic.LicenseClient) {
	stopable := cli.ShouldStop()
	if stopable {
		shouldStop += 1
	} else {
		shouldStop = 0
	}
}

func LicenseChecker(username string, password string) {
	cli := startLicenseClient(username, password)

	ticker := time.NewTicker(10 * time.Minute)
	killTicker := time.NewTicker(120 * time.Minute)

	for {
		select {
		case <-ticker.C:
			checkLicense(cli)
		case <-killTicker.C:
			if 0 < shouldStop {
				log.Infof("PLEASE CHECK YOU LICENSE VALIDATION AND COUNT LIMITATION OF %v/%v", username, password)
				os.Exit(-1)
			}
		}
	}
}
