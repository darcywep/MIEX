package common

import (
	"math/rand"
)

// Workload 负载类：生成已经转化为嵌套事务形式的五种TPC-C事务
type Workload struct {
	random      *rand.Rand
	txRandom    *rand.Rand
	txGenerator *TPCCTransaction
}

// NewWorkload 创建工作负载实例
func NewWorkload() *Workload {
	seed := uint64(0) // 简化实现，实际应该使用更好的种子
	random := rand.New(rand.NewSource(int64(seed)))
	txRandom := rand.New(rand.NewSource(int64(seed)))

	wl := &Workload{
		random:   random,
		txRandom: txRandom,
	}

	// 重置静态变量的值
	txGenerator := GetTPCCTransaction()
	txGenerator.ResetStatic()
	wl.txGenerator = txGenerator

	wl.init()
	return wl
}

// NewWorkloadWithSeed 使用指定种子创建工作负载实例
func NewWorkloadWithSeed(seed uint64) *Workload {
	random := rand.New(rand.NewSource(int64(seed)))
	txRandom := rand.New(rand.NewSource(int64(seed)))

	wl := &Workload{
		random:   random,
		txRandom: txRandom,
	}

	// 重置静态变量的值
	txGenerator := GetTPCCTransaction()
	txGenerator.ResetStatic()
	wl.txGenerator = txGenerator

	// 创建NewOrder事务生成器并生成一个事务
	newOrderTx := NewNewOrderTransaction(txRandom)
	newOrderTx.MakeTransaction()
	wl.txGenerator = newOrderTx.TPCCTransaction

	return wl
}

// NextTransaction 随机生成封装好的五类事务
func (wl *Workload) NextTransaction() *TPCCTransaction {
	option := wl.random.Intn(100) + 1

	switch {
	case option <= 45: // 生成由newOrder构成的负载
		newOrderTx := NewNewOrderTransaction(wl.txRandom)
		wl.txGenerator = newOrderTx.TPCCTransaction
		return newOrderTx.MakeTransaction()

	case option <= 88: // 生成由payment构成的负载
		paymentTx := NewPaymentTransaction(wl.txRandom)
		wl.txGenerator = paymentTx.TPCCTransaction
		return paymentTx.MakeTransaction()

	case option <= 92: // 生成由orderStatus构成的负载
		orderStatusTx := NewOrderStatusTransaction(wl.txRandom)
		wl.txGenerator = orderStatusTx.TPCCTransaction
		return orderStatusTx.MakeTransaction()

	case option <= 96: // 生成由delivery构成的负载
		deliveryTx := NewDeliveryTransaction(wl.txRandom)
		wl.txGenerator = deliveryTx.TPCCTransaction
		return deliveryTx.MakeTransaction()

	default: // 生成由stockLevel构成的负载
		stockLevelTx := NewStockLevelTransaction(wl.txRandom)
		wl.txGenerator = stockLevelTx.TPCCTransaction
		return stockLevelTx.MakeTransaction()
	}
}

// GetSeed 获取随机数种子
func (wl *Workload) GetSeed() int64 {
	// Go的rand包不直接提供获取种子的方法
	// 这里返回0作为占位符，实际实现可能需要修改
	return 0
}

// GetTxRandom 获取事务随机数生成器
func (wl *Workload) GetTxRandom() *rand.Rand {
	return wl.txRandom
}

// GetRandom 获取随机数生成器
func (wl *Workload) GetRandom() *rand.Rand {
	return wl.random
}

// SetSeed 设置随机数种子
func (wl *Workload) SetSeed(seed uint64) {
	// 重置静态变量的值
	wl.txGenerator.ResetStatic()

	// 重新初始化随机数生成器
	wl.random = rand.New(rand.NewSource(int64(seed)))
	wl.txRandom = rand.New(rand.NewSource(int64(seed)))

	// 重新初始化
	wl.init()
}

// init 初始化工作负载
func (wl *Workload) init() {
	// 创建NewOrder事务生成器并生成50个事务进行预热
	newOrderTx := NewNewOrderTransaction(wl.txRandom)
	wl.txGenerator = newOrderTx.TPCCTransaction

	for i := 0; i < 50; i++ {
		newOrderTx.MakeTransaction()
	}
}
