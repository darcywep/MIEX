package core

import (
	"chukonu/ethdb"
	"sync"

	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
)

type accessAddressChStruct struct {
	aam   *types.AccessAddressMap
	index int
}

func newAccessAddressChStruct(aam *types.AccessAddressMap, index int) *accessAddressChStruct {
	return &accessAddressChStruct{
		aam:   aam,
		index: index,
	}
}

// StmStateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StmStateProcessor implements Processor.
type StmStateProcessor struct {
	config  *params.ChainConfig // Chain configuration options
	chainDb ethdb.Database      // Canonical block chain
}

// NewStmStateProcessor initialises a new StmStateProcessor.
func NewStmStateProcessor(config *params.ChainConfig, chainDb ethdb.Database) *StmStateProcessor {
	return &StmStateProcessor{
		config:  config,
		chainDb: chainDb,
	}
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StmStateProcessor) Process(block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config) (*common.Hash, types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		gp          = new(GasPool).AddGas(block.GasLimit())
	)

	blockContext := NewEVMBlockContext(header, p.chainDb, nil)

	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		stmTxDB := state.NewStmTransaction(tx, i, 0, stmStateDB)
		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, stmTxDB, p.config, cfg)
		msg, err := TransactionToMessage(tx, types.MakeSigner(p.config, header.Number), header.BaseFee)
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		receipt, err := applyStmTransaction(msg, p.config, gp, stmStateDB, stmTxDB, blockNumber, blockHash, tx, usedGas, vmenv)
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		stmTxDB.Validation(true)
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		//root := stmStateDB.IntermediateRoot(true, i)
		//fmt.Println(i, root)
	}
	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Time()) {
		return nil, nil, nil, 0, fmt.Errorf("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	StmAccumulateRewards(p.config, stmStateDB, header, block.Uncles())

	root := stmStateDB.IntermediateRoot(p.config.IsEIP158(header.Number))
	return &root, receipts, allLogs, *usedGas, nil
}

func (p *StmStateProcessor) ProcessConcurrently(block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config) *[]*types.AccessAddressMap {
	var (
		//receipts    types.Receipts
		//usedGas     = new(uint64)
		header = block.Header()
		//blockHash   = block.Hash()
		//blockNumber = block.Number()
		//allLogs     []*types.Log
		//gp          = new(GasPool).AddGas(block.GasLimit())

		txsLen           = block.Transactions().Len()
		txsAccessAddress = make([]*types.AccessAddressMap, txsLen)
		wg               sync.WaitGroup
		accessAddrChan   = make(chan *accessAddressChStruct, 1024)
	)
	blockContext := NewEVMBlockContext(header, p.chainDb, nil)
	// Iterate over and process the individual transactions
	wg.Add(txsLen)
	for i, tx := range block.Transactions() {
		go p.applyConcurrent(header, tx, i, stmStateDB, &blockContext, cfg, accessAddrChan, &wg)
	}
	received := 0
	for aam := range accessAddrChan {
		txsAccessAddress[aam.index] = aam.aam
		received += 1
		if received == txsLen {
			close(accessAddrChan)
		}
	}
	wg.Wait()
	//for i, accessNormal := range txsAccessAddress {
	//	for addr, slotNormal := range *accessNormal {
	//		fmt.Println(i, addr, slotNormal.Slots)
	//	}
	//}
	return &txsAccessAddress
}

func (p *StmStateProcessor) applyConcurrent(header *types.Header, tx *types.Transaction, i int, stmStateDB *state.StmStateDB, blockContext *vm.BlockContext, cfg vm.Config, accessAddrChan chan *accessAddressChStruct, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		blockHash   = header.Hash()
		blockNumber = header.Number
		//allLogs     []*types.Log
		gp = new(GasPool).AddGas(header.GasLimit)
	)

	stmTxDB := state.NewStmTransaction(tx, i, 0, stmStateDB)
	vmenv := vm.NewEVM(*blockContext, vm.TxContext{}, stmTxDB, p.config, cfg)
	msg, _ := TransactionToMessage(tx, types.MakeSigner(p.config, header.Number), header.BaseFee)

	// 避免Nonce错误
	stmTxDB.SetNonce(msg.From, msg.Nonce)

	receipt, err := applyStmTransaction(msg, p.config, gp, stmStateDB, stmTxDB, blockNumber, blockHash, tx, usedGas, vmenv)
	if err != nil {
		fmt.Println(err)
	}
	receipts = append(receipts, receipt)
	//allLogs = append(allLogs, receipt.Logs...)

	//if i == 154 {
	//	for addr, slotNormal := range *stmTxDB.AccessAddress() {
	//		fmt.Println(i, addr, slotNormal.Slots)
	//	}
	//}

	accessAddrChan <- newAccessAddressChStruct(stmTxDB.AccessAddress(), i)
}

func applyStmTransaction(msg *Message, config *params.ChainConfig, gp *GasPool, stmStateDB *state.StmStateDB, statedb *state.StmTransaction, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, error) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	result, err := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}

	// Update the state with pending changes.
	var root []byte
	//if config.IsByzantium(blockNumber) {
	//	stmStateDB.Finalise(true, statedb.Index)
	//} else {
	//	root = stmStateDB.IntermediateRoot(config.IsEIP158(blockNumber), statedb.Index).Bytes()
	//}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash(), blockNumber.Uint64(), blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = blockHash
	receipt.BlockNumber = blockNumber
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, err
}

// StmAccumulateRewards credits the coinbase of the given block with the mining
// reward. The total reward consists of the static block reward and rewards for
// included uncles. The coinbase of each uncle block is also rewarded.
func StmAccumulateRewards(config *params.ChainConfig, state *state.StmStateDB, header *types.Header, uncles []*types.Header) {
	// Ethash proof-of-work protocol constants.
	var (
		FrontierBlockReward       = big.NewInt(5e+18) // Block reward in wei for successfully mining a block
		ByzantiumBlockReward      = big.NewInt(3e+18) // Block reward in wei for successfully mining a block upward from Byzantium
		ConstantinopleBlockReward = big.NewInt(2e+18) // Block reward in wei for successfully mining a block upward from Constantinople

		big8  = big.NewInt(8)
		big32 = big.NewInt(32)
	)

	// Select the correct block reward based on chain progression
	blockReward := FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ByzantiumBlockReward
	}
	if config.IsConstantinople(header.Number) {
		blockReward = ConstantinopleBlockReward
	}
	// Accumulate the rewards for the miner and any included uncles
	reward := new(big.Int).Set(blockReward)
	r := new(big.Int)
	for _, uncle := range uncles {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, blockReward)
		r.Div(r, big8)
		state.AddBalance(uncle.Coinbase, r)

		r.Div(blockReward, big32)
		reward.Add(reward, r)
	}
	state.AddBalance(header.Coinbase, reward)
}
