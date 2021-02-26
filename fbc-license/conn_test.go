package fbclicense

import (
	"flag"
	log "github.com/EntropyPool/entropy-logger"
	"testing"
	"time"
)

var (
	clientSn     = flag.String("clientSn", "hello!", "client software SN")
	systemSn     = flag.String("systemSn", "1234567890", "system sn")
	serverSocket = flag.String("serverSocket", "47.99.107.242:8097", "server socket")
)

func init() {
	log.SetLevel("debug")
}

func TestLicenseClient(t *testing.T) {
	flag.Parse()

	configMap := make(map[string]string)
	configMap["clientSn"] = "hello!"
	configMap["systemSn"] = "123456790"
	configMap["serverSocket"] = *serverSocket

	gClient := NewGuardClient(configMap)
	go gClient.Run()

	for i := 0; i < 10; i += 1 {
		validate := gClient.Validate()
		log.Infof(log.Fields{}, "Validate: %v", validate)
		shouldStop := gClient.ShouldStop()
		log.Infof(log.Fields{}, "ShouldStop: %v", shouldStop)
		time.Sleep(10 * time.Second)
	}

	configMap = make(map[string]string)
	configMap["clientSn"] = "test"
	configMap["systemSn"] = "123456790"
	configMap["serverSocket"] = *serverSocket

	gClient = NewGuardClient(configMap)
	go gClient.Run()

	for i := 0; i < 10; i += 1 {
		validate := gClient.Validate()
		log.Infof(log.Fields{}, "Validate: %v", validate)
		shouldStop := gClient.ShouldStop()
		log.Infof(log.Fields{}, "ShouldStop: %v", shouldStop)
		time.Sleep(10 * time.Second)
	}
}
