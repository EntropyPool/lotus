package fbclicense

import (
	log "github.com/EntropyPool/entropy-logger"
	"testing"
	"time"
)

func init() {
	log.SetLevel("debug")
}

func TestLicenseClient(t *testing.T) {
	gClient := NewLicenseClient(LicenseConfig{
		ClientUser:    "hello!",
		ClientSn:      "123456790",
		LicenseServer: "localhost:8099",
		Scheme:        "http",
	})
	go gClient.Run()

	for i := 0; i < 10; i += 1 {
		validate := gClient.Validate()
		log.Infof(log.Fields{}, "Validate: %v", validate)
		shouldStop := gClient.ShouldStop()
		log.Infof(log.Fields{}, "ShouldStop: %v", shouldStop)
		time.Sleep(10 * time.Second)
	}

	gClient = NewLicenseClient(LicenseConfig{
		ClientUser:    "test",
		ClientSn:      "123456790",
		LicenseServer: "localhost:8099",
		Scheme:        "http",
	})
	go gClient.Run()

	for i := 0; i < 10; i += 1 {
		validate := gClient.Validate()
		log.Infof(log.Fields{}, "Validate: %v", validate)
		shouldStop := gClient.ShouldStop()
		log.Infof(log.Fields{}, "ShouldStop: %v", shouldStop)
		time.Sleep(10 * time.Second)
	}
}
