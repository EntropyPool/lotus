package main

import (
	machspec "github.com/EntropyPool/machine-spec"
	lic "github.com/filecoin-project/lotus/fbc-license"
	_ "os"
	"time"
)

var shouldStop = false

func startLicenseClient(username string) *lic.GuardClient {
	spec := machspec.NewMachineSpec()
	spec.PrepareLowLevel()
	sn := spec.SN()

	configMap := make(map[string]string)
	configMap["clientSn"] = username
	configMap["systemSn"] = sn
	configMap["serverSocket"] = "47.99.107.242:8097"

	cli := lic.NewGuardClient(configMap)
	go cli.Run()

	return cli
}

func checkLicense(cli *lic.GuardClient) {
	shouldStop = false

	validate := cli.Validate()
	if validate {
		shouldStop = true
		return
	}
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
				// os.Exit(-1)
			}
		}
	}
}
