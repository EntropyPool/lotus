package fbclicense

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "github.com/EntropyPool/entropy-logger"
	machspec "github.com/EntropyPool/machine-spec"
	crypto "github.com/NpoolDevOps/fbc-license-service/crypto"
	fbctypes "github.com/NpoolDevOps/fbc-license-service/types"
	httpdaemon "github.com/NpoolRD/http-daemon"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	"time"
)

const (
	ExchangeKey = 1
	Login       = 2
	Running     = 3
)

type LicenseClient struct {
	RemoteRsaObj   *crypto.RsaCrypto
	LocalRsaObj    *crypto.RsaCrypto
	sessionId      uuid.UUID
	clientUser     string
	clientUserPass string
	networkType    string
	clientSn       string
	licenseServer  string
	clientUuid     uuid.UUID
	state          int
	shouldStop     bool
	scheme         string
}

type LicenseConfig struct {
	ClientUser     string
	ClientUserPass string
	ClientSn       string
	LicenseServer  string
	NetworkType    string
	Scheme         string
}

func NewLicenseClient(config LicenseConfig) *LicenseClient {
	passHash := sha256.Sum256([]byte(config.ClientUserPass))
	log.Infof(log.Fields{}, "%x / %v", passHash, config.ClientUserPass)
	return &LicenseClient{
		LocalRsaObj:    crypto.NewRsaCrypto(2048),
		clientUser:     config.ClientUser,
		clientUserPass: hex.EncodeToString(passHash[0:])[0:12],
		networkType:    config.NetworkType,
		clientSn:       config.ClientSn,
		licenseServer:  config.LicenseServer,
		state:          ExchangeKey,
		shouldStop:     true,
		scheme:         config.Scheme,
	}
}

func (self *LicenseClient) Exchangekey() error {
	targetUri := fmt.Sprintf("%v://%v%v", self.scheme, self.licenseServer, fbctypes.ExchangeKeyAPI)

	spec := machspec.NewMachineSpec()
	spec.PrepareLowLevel()

	resp, err := httpdaemon.R().
		SetHeader("Content-Type", "application/json").
		SetBody(fbctypes.ExchangeKeyInput{
			PublicKey: string(self.LocalRsaObj.GetPubkey()),
			Spec:      spec.SN(),
		}).
		Post(targetUri)
	if err != nil {
		log.Errorf(log.Fields{}, "exchange key error: %v", err)
		return err
	}

	apiResp, err := httpdaemon.ParseResponse(resp)
	if err != nil {
		log.Errorf(log.Fields{}, "exchange api response error: %v", err)
		return err
	}

	if apiResp.Code != 0 {
		log.Errorf(log.Fields{}, "exchange api response error: %v", apiResp.Msg)
		return xerrors.Errorf(apiResp.Msg)
	}

	if apiResp.Body == nil {
		log.Errorf(log.Fields{}, "client exchange response error: empty body")
		return xerrors.Errorf("client exchange response error: empty body")
	}

	output := fbctypes.ExchangeKeyOutput{}
	b, _ := json.Marshal(apiResp.Body)
	err = json.Unmarshal(b, &output)
	if err != nil {
		log.Errorf(log.Fields{}, "parse api response error: %v", err)
		return err
	}

	self.RemoteRsaObj = crypto.NewRsaCryptoWithParam([]byte(output.PublicKey), nil)
	self.sessionId = output.SessionId
	self.state = Login

	return nil
}

func (self *LicenseClient) Login() error {
	targetUri := fmt.Sprintf("%v://%v%v", self.scheme, self.licenseServer, fbctypes.LoginAPI)

	input := fbctypes.ClientLoginInput{
		ClientUser:   self.clientUser,
		ClientPasswd: self.clientUserPass,
		ClientSN:     self.clientSn,
		NetworkType:  self.networkType,
	}
	input.SessionId = self.sessionId

	resp, err := httpdaemon.R().
		SetHeader("Content-Type", "application/json").
		SetBody(input).
		Post(targetUri)
	if err != nil {
		log.Errorf(log.Fields{}, "client login response error: %v", err)
		return err
	}

	apiResp, err := httpdaemon.ParseResponse(resp)
	if err != nil {
		log.Errorf(log.Fields{}, "client login response error: %v", err)
		return err
	}

	if apiResp.Code != 0 {
		log.Errorf(log.Fields{}, "client login response error: %v", apiResp.Msg)
		return xerrors.Errorf(apiResp.Msg)
	}

	if apiResp.Body == nil {
		log.Errorf(log.Fields{}, "client login response error: empty body")
		return xerrors.Errorf("client login response error: empty body")
	}

	var output = fbctypes.ClientLoginOutput{}

	b, _ := json.Marshal(apiResp.Body)
	err = json.Unmarshal(b, &output)
	if err != nil {
		log.Errorf(log.Fields{}, "client login parse response: %v", err)
		return err
	}

	self.state = Running
	self.clientUuid = output.ClientUuid

	return nil
}

func (self *LicenseClient) Heartbeat() error {
	targetUri := fmt.Sprintf("%v://%v%v", self.scheme, self.licenseServer, fbctypes.HeartbeatV1API)

	input := fbctypes.HeartbeatInput{
		ClientUuid: self.clientUuid,
	}
	input.SessionId = self.sessionId

	resp, err := httpdaemon.R().
		SetHeader("Content-Type", "application/json").
		SetBody(input).
		Post(targetUri)
	if err != nil {
		log.Errorf(log.Fields{}, "heartbeat error: %v", err)
		self.shouldStop = true
		return err
	}

	apiResp, err := httpdaemon.ParseResponse(resp)
	if err != nil {
		log.Errorf(log.Fields{}, "heartbeat api response error: %v", err)
		return err
	}

	if apiResp.Code != 0 {
		log.Errorf(log.Fields{}, "client heartbeat response error: %v", apiResp.Msg)
		return xerrors.Errorf(apiResp.Msg)
	}

	if apiResp.Body == nil {
		log.Errorf(log.Fields{}, "client heartbeat response error: empty body")
		return xerrors.Errorf("client heartbeat response error: empty body")
	}

	// body := apiResp.Body
	// hBody, _ := hex.DecodeString(body.(string))
	// data, _ := self.LocalRsaObj.Decrypt([]byte(hBody))

	var output = fbctypes.HeartbeatOutput{}

	b, _ := json.Marshal(apiResp.Body)
	err = json.Unmarshal(b, &output)
	if err != nil {
		log.Errorf(log.Fields{}, "heartbeat parse response error: %v", err)
		return err
	}

	self.shouldStop = output.ShouldStop

	return nil
}

func (self *LicenseClient) Validate() bool {
	switch self.state {
	case Running:
		return true
	}
	return false
}

func (self *LicenseClient) ShouldStop() bool {
	return self.shouldStop
}

func (self *LicenseClient) Run() {
	ticker1 := time.NewTicker(10 * time.Second)
	ticker2 := time.NewTicker(10 * time.Second)
	for {
		switch self.state {
		case ExchangeKey:
			self.Exchangekey()
			<-ticker2.C
		case Login:
			self.Login()
			<-ticker2.C
		case Running:
			self.Heartbeat()
			<-ticker1.C
		}
	}
}
