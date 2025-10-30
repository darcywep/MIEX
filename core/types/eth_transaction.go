package types

import (
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
)

type EthTransaction struct {
	data txdata
	// caches
	hash atomic.Value
	size atomic.Value
	from atomic.Value
}

type txdata struct {
	Nonce    uint64   // nonce of sender account
	GasPrice *big.Int // wei per gas
	Gas      uint64   // gas limit
	From     *common.Address
	To       *common.Address `rlp:"nil"` // nil means contract creation
	Value    *big.Int        // wei amount
	Data     []byte          // contract invocation input data
	V, R, S  *big.Int        // signature values

	// This is only used when marshaling to JSON.
	Hash *common.Hash `json:"hash" rlp:"-"`
}

func NewEthTransaction(nonce uint64, from, to *common.Address, amount *big.Int, data []byte) *EthTransaction {
	return newEthTransaction(nonce, from, to, amount, data)
}

func NewContractCreation(nonce uint64, from *common.Address, amount *big.Int, data []byte) *EthTransaction {
	return newEthTransaction(nonce, from, nil, amount, data)
}

func newEthTransaction(nonce uint64, from, to *common.Address, amount *big.Int, data []byte) *EthTransaction {
	if len(data) > 0 {
		data = common.CopyBytes(data)
	}
	d := txdata{
		Nonce:    nonce,
		From:     from,
		To:       to,
		Data:     data,
		Value:    new(big.Int),
		Gas:      1e18,
		GasPrice: new(big.Int).SetInt64(1),
		V:        new(big.Int).SetInt64(0),
		R:        new(big.Int).SetInt64(0),
		S:        new(big.Int).SetInt64(0),
	}
	if amount != nil {
		d.Value.Set(amount)
	}

	return &EthTransaction{data: d}
}

func (tx *EthTransaction) Data() []byte         { return common.CopyBytes(tx.data.Data) }
func (tx *EthTransaction) Gas() uint64          { return tx.data.Gas }
func (tx *EthTransaction) GasPrice() *big.Int   { return new(big.Int).Set(tx.data.GasPrice) }
func (tx *EthTransaction) Value() *big.Int      { return new(big.Int).Set(tx.data.Value) }
func (tx *EthTransaction) Nonce() uint64        { return tx.data.Nonce }
func (tx *EthTransaction) CheckNonce() bool     { return true }
func (tx *EthTransaction) From() common.Address { return *tx.data.From }

// To returns the recipient address of the transaction.
// It returns nil if the transaction is a contract creation.
func (tx *EthTransaction) To() *common.Address {
	if tx.data.To == nil {
		return nil
	}
	to := *tx.data.To
	return &to
}

// Hash hashes the RLP encoding of tx.
// It uniquely identifies the transaction.
func (tx *EthTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}

	var v common.Hash
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, tx)
	hw.Sum(v[:0])
	tx.hash.Store(v)
	return v
}

// Size returns the true RLP encoded storage size of the transaction, either by
// encoding and returning it, or returning a previsouly cached value.
func (tx *EthTransaction) Size() common.StorageSize {
	if size := tx.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := writeCounter(0)
	rlp.Encode(&c, &tx.data)
	tx.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

type writeCounter common.StorageSize

func (c *writeCounter) Write(b []byte) (int, error) {
	*c += writeCounter(len(b))
	return len(b), nil
}

// AsMessage returns the transaction as a core.Message.
//
// AsMessage requires a signer to derive the sender.
//
// XXX Rename message to something less arbitrary?
//func (tx *EthTransaction) AsMessage() types.Message {
//	msg := types.NewMessage(*tx.data.From, tx.data.Recipient, tx.data.AccountNonce, tx.data.Amount,
//		tx.data.GasLimit, new(big.Int).Set(tx.data.Price), tx.data.Payload, true)
//
//	return msg
//}
