// Package evm is a higher level wrapper for the EVM and the related
// StateDB.
// evm.go contains methods for creating the evm and deploying/calling
// contracts.
// db.go contains methods for interacting with the stateDB i.e. getting
// accounts and balances
package levm

import (
	"chukonu/core/state"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Account is a simple representation of an
// account in the stateDB, for further
// controls call DB() to retrieve the db
// reference itself.
// Almost as a snapshot
type Account struct {
	Balance *big.Int
	Nonce   uint64
	Code    []byte
}

// DB returns a pointer to the vm's state DB.
func (lvm *LEVM) DB() *state.StateDB {
	return lvm.allDBForState.StateDB
}

// GetAccount returns a copy of stateDB
func (lvm *LEVM) GetAccount(addr common.Address) Account {
	acc := Account{}
	lvm.allDBForState.StateDB.GetOrNewStateObject(addr)
	acc.Balance = lvm.allDBForState.StateDB.GetBalance(addr)
	acc.Nonce = lvm.allDBForState.StateDB.GetNonce(addr)
	acc.Code = lvm.allDBForState.StateDB.GetCode(addr)
	return acc
}

// SetAccount will update the statedb with
// related values contained in the Account
// snapshot.
func (lvm *LEVM) SetAccount(addr common.Address, acc Account) {
	lvm.allDBForState.StateDB.GetOrNewStateObject(addr)
	lvm.allDBForState.StateDB.SetBalance(addr, acc.Balance)
	lvm.allDBForState.StateDB.SetNonce(addr, acc.Nonce)
	lvm.allDBForState.StateDB.SetCode(addr, acc.Code)
}

// NewAccount Create a new Account and Set its balance
func (lvm *LEVM) NewAccount(addr common.Address, balance *big.Int) {
	lvm.allDBForState.StateDB.GetOrNewStateObject(addr)
	lvm.allDBForState.StateDB.SetBalance(addr, balance)
}
