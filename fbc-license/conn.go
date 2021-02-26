package fbclicense

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "github.com/EntropyPool/entropy-logger"
	httpdaemon "github.com/NpoolRD/http-daemon"
	"github.com/filecoin-project/lotus/rsa_crypto"
	"time"
)

var (
	exchangeUrl  = "/api_client/exchange_key"
	testUrl      = "/api_client/test"
	startUpUrl   = "/api_client/startup"
	heartBeatUrl = "/api_client/heartbeat"
)

const (
	ExchangeKey = 1
	StartUp     = 2
	Running     = 3
)

type GuardClient struct {
	RemoteRsaObj *rsa_crypto.RsaCrypto
	LocalRsaObj  *rsa_crypto.RsaCrypto
	sessionId    string
	clientSn     string
	systemSn     string
	serverSocket string
	SoftwareUuid string
	state        int
	shouldStop   bool
}

func NewGuardClient(config map[string]string) *GuardClient {
	localRsaObj := rsa_crypto.NewRsaCrypto(2048)

	return &GuardClient{
		LocalRsaObj:  localRsaObj,
		state:        ExchangeKey,
		clientSn:     config["clientSn"],
		systemSn:     config["systemSn"],
		serverSocket: config["serverSocket"],
		shouldStop:   true,
	}
}

func (self *GuardClient) Exchangekey() error {
	param := make(map[string]interface{})
	param["public_key"] = string(self.LocalRsaObj.GetPubkey())
	targetUri := fmt.Sprintf("http://%v%v", self.serverSocket, exchangeUrl)

	resp, err := httpdaemon.R().
		SetHeader("Content-Type", "application/json").
		SetBody(param).
		Post(targetUri)
	if err != nil {
		log.Errorf(log.Fields{}, "exchange key error [%v] (%v)", param, err)
		return err
	}

	apiResp, err := httpdaemon.ParseResponse(resp)
	if err != nil {
		log.Errorf(log.Fields{}, "exchange api response error [%v] (%v)", param, err)
		return err
	}

	pubKey := apiResp.Body.(map[string]interface{})["public_key"]
	remoteRsaObj := rsa_crypto.NewRsaCryptoWithParam([]byte(pubKey.(string)), nil)
	self.RemoteRsaObj = remoteRsaObj
	sessionId := apiResp.Body.(map[string]interface{})["sessionId"]
	self.sessionId = string(sessionId.(string))

	self.state = StartUp

	return nil
}

func (self *GuardClient) StartUpClient() error {
	targetUri := fmt.Sprintf("http://%v%v", self.serverSocket, startUpUrl)

	param := make(map[string]interface{})
	param["ClientSn"] = self.clientSn
	param["SystemSn"] = self.systemSn

	jparam, err := json.Marshal(param)
	if err != nil {
		log.Errorf(log.Fields{}, "setup client (marshal param) %v [%v] (%v)", targetUri, param, err)
		return err
	}

	ciphertext, err := self.RemoteRsaObj.Encrypt([]byte(jparam))
	req := make(map[string]interface{})
	req["data"] = hex.EncodeToString(ciphertext)
	req["sessionId"] = self.sessionId

	resp, err := httpdaemon.R().
		SetHeader("Content-Type", "application/json").
		SetBody(req).
		Post(targetUri)
	if err != nil {
		log.Errorf(log.Fields{}, "setup client error [%v] (%v)", param, err)
		return err
	}

	apiResp, err := httpdaemon.ParseResponse(resp)
	if err != nil {
		log.Errorf(log.Fields{}, "setup client api response error [%v] (%v)", param, err)
		return err
	}

	ctext := apiResp.Body.(string)
	htext, _ := hex.DecodeString(ctext)
	data, _ := self.LocalRsaObj.Decrypt([]byte(htext))

	var startupMap map[string]interface{}
	err = json.Unmarshal(data, &startupMap)
	if err != nil {
		log.Errorf(log.Fields{}, "setup client (parse response) [%v] (%v)", param, err)
		return err
	}

	if startupMap["startUp"] == true {
		self.state = Running
		self.SoftwareUuid = startupMap["softwareUuid"].(string)
	}

	return nil
}

func (self *GuardClient) SendHeartBeat() error {
	reqParam := make(map[string]interface{})
	reqParam["sessionId"] = self.sessionId
	reqParam["softwareUuid"] = self.SoftwareUuid
	targetUri := fmt.Sprintf("http://%v%v", self.serverSocket, heartBeatUrl)

	resp, err := httpdaemon.R().
		SetHeader("Content-Type", "application/json").
		SetBody(reqParam).
		Post(targetUri)
	if err != nil {
		log.Errorf(log.Fields{}, "heartbeat error [%v] (%v)", reqParam, err)
		return err
	}

	apiResp, err := httpdaemon.ParseResponse(resp)
	if err != nil {
		log.Errorf(log.Fields{}, "heartbeat api response error [%v] (%v)", reqParam, err)
		return err
	}

	body := apiResp.Body
	hBody, _ := hex.DecodeString(body.(string))
	data, _ := self.LocalRsaObj.Decrypt([]byte(hBody))

	var heartbeatMap map[string]interface{}
	err = json.Unmarshal(data, &heartbeatMap)
	if err != nil {
		log.Errorf(log.Fields{}, "heartbeat (parse response) [%v] (%v)", reqParam, err)
		return err
	}

	if _, ok := heartbeatMap["stop"]; ok {
		self.shouldStop = heartbeatMap["stop"].(bool)
	}

	return nil
}

func (self *GuardClient) Validate() bool {
	switch self.state {
	case Running:
		return true
	}
	return false
}

func (self *GuardClient) ShouldStop() bool {
	return self.shouldStop
}

func (self *GuardClient) Run() {
	ticker1 := time.NewTicker(10 * time.Second)
	ticker2 := time.NewTicker(10 * time.Second)
	for {
		switch self.state {
		case ExchangeKey:
			self.Exchangekey()
			<-ticker2.C
		case StartUp:
			self.StartUpClient()
			<-ticker2.C
		case Running:
			self.SendHeartBeat()
			<-ticker1.C
		}
	}
}
