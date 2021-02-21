package main

import (
	machspec "github.com/EntropyPool/machine-spec"
	lic "github.com/NpoolRD/fbc-license"
	"os"
	"time"
)

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
	validate := cli.Validate()
	if validate {
		return
	}
	shouldStop := cli.ShouldStop()
	if shouldStop {
		os.Exit(-1)
	}
}

func LicenseChecker(username string) {
	cli := startLicenseClient(username)

	ticker := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-ticker.C:
			checkLicense(cli)
		}
	}
}
