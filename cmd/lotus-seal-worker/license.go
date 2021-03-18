package main

import (
	machspec "github.com/EntropyPool/machine-spec"
	lic "github.com/filecoin-project/lotus/fbc-license"
	"os"
	"time"
)

var shouldStop = false

func startLicenseClient(username string) *lic.LicenseClient {
	spec := machspec.NewMachineSpec()
	spec.PrepareLowLevel()
	sn := spec.SN()

	cli := lic.NewLicenseClient(lic.LicenseConfig{
		ClientUser:    username,
		ClientSn:      sn,
		LicenseServer: "license.npool.top",
		Scheme:        "https",
	})
	go cli.Run()

	return cli
}

func checkLicense(cli *lic.LicenseClient) {
	shouldStop := cli.ShouldStop()
	if shouldStop {
		shouldStop = true
	}
}

func LicenseChecker(username string) {
	cli := startLicenseClient(username)

	ticker := time.NewTicker(10 * time.Minute)
	killTimer := time.NewTimer(60 * time.Minute)

	for {
		select {
		case <-ticker.C:
			checkLicense(cli)
		case <-killTimer.C:
			if shouldStop {
				os.Exit(-1)
			}
		}
	}
}
