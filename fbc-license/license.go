package fbclicense

import (
	log "github.com/EntropyPool/entropy-logger"
	machspec "github.com/EntropyPool/machine-spec"
	"os"
	"time"
)

var shouldStop = 0

func startLicenseClient(username string, password string, appName string) *LicenseClient {
	spec := machspec.NewMachineSpec()
	spec.PrepareLowLevel()
	sn := spec.SN()

	cli := NewLicenseClient(LicenseConfig{
		ClientUser:     username,
		ClientUserPass: password,
		NetworkType:    appName,
		ClientSn:       sn,
		LicenseServer:  "license.npool.top",
		Scheme:         "https",
	})
	go cli.Run()

	return cli
}

func checkLicense(cli *LicenseClient) {
	stopable := cli.ShouldStop()
	if stopable {
		shouldStop += 1
	} else {
		shouldStop = 0
	}
}

func LicenseChecker(username string, password string, stoppable bool, appName string) {
	cli := startLicenseClient(username, password, appName)

	ticker := time.NewTicker(10 * time.Minute)
	killTicker := time.NewTicker(120 * time.Minute)

	for {
		select {
		case <-ticker.C:
			checkLicense(cli)
		case <-killTicker.C:
			if 0 < shouldStop && stoppable {
				log.Infof(log.Fields{}, "PLEASE CHECK YOU LICENSE VALIDATION AND COUNT LIMITATION OF %v/%v", username, password)
				os.Exit(-1)
			}
		}
	}
}
