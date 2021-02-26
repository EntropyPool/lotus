package rsa_crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
)

type RsaCrypto struct {
	Pubkey  []byte
	Privkey []byte
	Keylen  int
}

func GenerateRsakey(keylen int) ([]byte, []byte) {
	privateKey, err := rsa.GenerateKey(rand.Reader, keylen)
	if err != nil {
		panic(err)
	}
	derStream := x509.MarshalPKCS1PrivateKey(privateKey)
	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: derStream,
	}
	prvkey := pem.EncodeToMemory(block)
	publicKey := &privateKey.PublicKey
	derPkix, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		panic(err)
	}
	block = &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: derPkix,
	}
	pubkey := pem.EncodeToMemory(block)
	return pubkey, prvkey
}

func NewRsaCrypto(keylen int) *RsaCrypto {

	pubkey, prvkey := GenerateRsakey(keylen)

	return &RsaCrypto{
		Pubkey:  pubkey,
		Privkey: prvkey,
		Keylen:  keylen,
	}
}

func NewRsaCryptoWithParam(pubkey []byte, privkey []byte) *RsaCrypto {

	return &RsaCrypto{
		Pubkey:  pubkey,
		Privkey: privkey,
	}
}

func (self *RsaCrypto) Encrypt(content []byte) ([]byte, error) {
	block, _ := pem.Decode(self.Pubkey)
	if block == nil {
		return nil, errors.New("public key error")
	}
	pubInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pub := pubInterface.(*rsa.PublicKey)
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, pub, content)
	if err != nil {
		return nil, err
	}

	return ciphertext, nil
}

func (self *RsaCrypto) Decrypt(ciphertext []byte) ([]byte, error) {

	block, _ := pem.Decode(self.Privkey)
	if block == nil {
		return nil, errors.New("private key error")
	}
	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	data, err := rsa.DecryptPKCS1v15(rand.Reader, priv, ciphertext)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (self *RsaCrypto) GetPubkey() []byte {

	return self.Pubkey
}

func (self *RsaCrypto) GetPrivkey() []byte {

	return self.Privkey
}
