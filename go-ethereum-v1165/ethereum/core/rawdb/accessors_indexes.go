// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"math/big"

	"Janus/ethereum/core/types"
	"Janus/ethereum/ethdb"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// DecodeTxLookupEntry decodes the supplied tx lookup data.
func DecodeTxLookupEntry(data []byte, db ethdb.Reader) *uint64 {
	// Database v6 tx lookup just stores the block number
	if len(data) < common.HashLength {
		number := new(big.Int).SetBytes(data).Uint64()
		return &number
	}
	// Database v4-v5 tx lookup format just stores the hash
	if len(data) == common.HashLength {
		number, ok := ReadHeaderNumber(db, common.BytesToHash(data))
		if !ok {
			return nil
		}
		return &number
	}
	// Finally try database v3 tx lookup format
	var entry LegacyTxLookupEntry
	if err := rlp.DecodeBytes(data, &entry); err != nil {
		log.Error("Invalid transaction lookup entry RLP", "blob", data, "err", err)
		return nil
	}
	return &entry.BlockIndex
}

// ReadTxLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the transaction or receipt by hash.
func ReadTxLookupEntry(db ethdb.Reader, hash common.Hash) *uint64 {
	data, _ := db.Get(txLookupKey(hash))
	if len(data) == 0 {
		return nil
	}
	return DecodeTxLookupEntry(data, db)
}

// writeTxLookupEntry stores a positional metadata for a transaction,
// enabling hash based transaction and receipt lookups.
func writeTxLookupEntry(db ethdb.KeyValueWriter, hash common.Hash, numberBytes []byte) {
	if err := db.Put(txLookupKey(hash), numberBytes); err != nil {
		log.Crit("Failed to store transaction lookup entry", "err", err)
	}
}

// WriteTxLookupEntries is identical to WriteTxLookupEntry, but it works on
// a list of hashes
func WriteTxLookupEntries(db ethdb.KeyValueWriter, number uint64, hashes []common.Hash) {
	numberBytes := new(big.Int).SetUint64(number).Bytes()
	for _, hash := range hashes {
		writeTxLookupEntry(db, hash, numberBytes)
	}
}

// WriteTxLookupEntriesByBlock stores a positional metadata for every transaction from
// a block, enabling hash based transaction and receipt lookups.
func WriteTxLookupEntriesByBlock(db ethdb.KeyValueWriter, block *types.Block) {
	numberBytes := block.Number().Bytes()
	for _, tx := range block.Transactions() {
		writeTxLookupEntry(db, tx.Hash(), numberBytes)
	}
}

// DeleteTxLookupEntry removes all transaction data associated with a hash.
func DeleteTxLookupEntry(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Delete(txLookupKey(hash)); err != nil {
		log.Crit("Failed to delete transaction lookup entry", "err", err)
	}
}

// DeleteTxLookupEntries removes all transaction lookups for a given block.
func DeleteTxLookupEntries(db ethdb.KeyValueWriter, hashes []common.Hash) {
	for _, hash := range hashes {
		DeleteTxLookupEntry(db, hash)
	}
}

// DeleteAllTxLookupEntries purges all the transaction indexes in the database.
// If condition is specified, only the entry with condition as True will be
// removed; If condition is not specified, the entry is deleted.
func DeleteAllTxLookupEntries(db ethdb.KeyValueStore, condition func(common.Hash, []byte) bool) {
	iter := NewKeyLengthIterator(db.NewIterator(txLookupPrefix, nil), common.HashLength+len(txLookupPrefix))
	defer iter.Release()

	batch := db.NewBatch()
	for iter.Next() {
		txhash := common.Hash(iter.Key()[1:])
		if condition == nil || condition(txhash, iter.Value()) {
			batch.Delete(iter.Key())
		}
		if batch.ValueSize() >= ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				log.Crit("Failed to delete transaction lookup entries", "err", err)
			}
			batch.Reset()
		}
	}
	if batch.ValueSize() > 0 {
		if err := batch.Write(); err != nil {
			log.Crit("Failed to delete transaction lookup entries", "err", err)
		}
		batch.Reset()
	}
}

// FilterMapsRange is a storage representation of the block range covered by the
// filter maps structure and the corresponting log value index range.
type FilterMapsRange struct {
	Version                      uint32
	HeadIndexed                  bool
	HeadDelimiter                uint64
	BlocksFirst, BlocksAfterLast uint64
	MapsFirst, MapsAfterLast     uint32
	TailPartialEpoch             uint32
}

// ReadFilterMapsRange retrieves the filter maps range data. Note that if the
// database entry is not present, that is interpreted as a valid non-initialized
// state and returns a blank range structure and no error.
func ReadFilterMapsRange(db ethdb.KeyValueReader) (FilterMapsRange, bool, error) {
	if has, err := db.Has(filterMapsRangeKey); err != nil || !has {
		return FilterMapsRange{}, false, err
	}
	encRange, err := db.Get(filterMapsRangeKey)
	if err != nil {
		return FilterMapsRange{}, false, err
	}
	var fmRange FilterMapsRange
	if err := rlp.DecodeBytes(encRange, &fmRange); err != nil {
		return FilterMapsRange{}, false, err
	}

	return fmRange, true, nil
}
