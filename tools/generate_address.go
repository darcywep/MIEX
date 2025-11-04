package tools

import (
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func GenerateAddress() common.Address {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		log.Fatal(err)
	}

	// 公钥（未压缩，64 字节 X||Y）
	pubBytes := crypto.FromECDSAPub(&privateKey.PublicKey) // 65 bytes with 0x04 prefix
	// go-ethereum 返回的是 65 字节（0x04 + X + Y），但计算地址时要剥掉前缀 0x04
	if len(pubBytes) == 65 && pubBytes[0] == 0x04 {
		pubBytes = pubBytes[1:]
	}

	// 或者使用 go-ethereum 的 helper 直接得到地址：
	addr := crypto.PubkeyToAddress(privateKey.PublicKey)
	return addr
}
