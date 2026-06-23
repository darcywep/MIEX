package tools

import (
	janusConfig "Janus/config"
	"Janus/ethereum/core/types"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestApplyTxTypeMisclassificationFlipsScheduleTypeOnly(t *testing.T) {
	blockTxs := []types.Transactions{make(types.Transactions, 0, 10)}
	for i := 0; i < 10; i++ {
		tx := types.NewTransaction(uint64(i), common.Address{}, big.NewInt(0), 21000, big.NewInt(1), nil)
		if i%2 == 0 {
			tx.TxType = janusConfig.LongTx
		} else {
			tx.TxType = janusConfig.ShortTx
		}
		blockTxs[0] = append(blockTxs[0], tx)
	}

	stats, err := ApplyTxTypeMisclassification(blockTxs, 0.3, 7)
	if err != nil {
		t.Fatalf("ApplyTxTypeMisclassification returned error: %v", err)
	}
	if stats.CandidateTxs != 10 {
		t.Fatalf("CandidateTxs = %d, want 10", stats.CandidateTxs)
	}
	if stats.MisclassifiedTxs != 3 {
		t.Fatalf("MisclassifiedTxs = %d, want 3", stats.MisclassifiedTxs)
	}

	overrides := 0
	for i, tx := range blockTxs[0] {
		wantRealType := janusConfig.ShortTx
		if i%2 == 0 {
			wantRealType = janusConfig.LongTx
		}
		if tx.TxType != wantRealType {
			t.Fatalf("tx %d real TxType changed to %d, want %d", i, tx.TxType, wantRealType)
		}
		if !tx.HasScheduleTxTypeOverride() {
			continue
		}
		overrides++
		if tx.TxType == janusConfig.LongTx && tx.ScheduleTransactionType() != janusConfig.ShortTx {
			t.Fatalf("long tx %d schedule type = %d, want short", i, tx.ScheduleTransactionType())
		}
		if tx.TxType == janusConfig.ShortTx && tx.ScheduleTransactionType() != janusConfig.LongTx {
			t.Fatalf("short tx %d schedule type = %d, want long", i, tx.ScheduleTransactionType())
		}
	}
	if overrides != 3 {
		t.Fatalf("override count = %d, want 3", overrides)
	}
}

func TestApplyTxTypeMisclassificationClearsPreviousOverrides(t *testing.T) {
	tx := types.NewTransaction(0, common.Address{}, big.NewInt(0), 21000, big.NewInt(1), nil)
	tx.TxType = janusConfig.LongTx
	tx.SetScheduleTxType(janusConfig.ShortTx)

	stats, err := ApplyTxTypeMisclassification([]types.Transactions{{tx}}, 0, 1)
	if err != nil {
		t.Fatalf("ApplyTxTypeMisclassification returned error: %v", err)
	}
	if stats.MisclassifiedTxs != 0 {
		t.Fatalf("MisclassifiedTxs = %d, want 0", stats.MisclassifiedTxs)
	}
	if tx.HasScheduleTxTypeOverride() {
		t.Fatalf("expected schedule type override to be cleared")
	}
	if tx.ScheduleTransactionType() != janusConfig.LongTx {
		t.Fatalf("ScheduleTransactionType = %d, want long", tx.ScheduleTransactionType())
	}
}
