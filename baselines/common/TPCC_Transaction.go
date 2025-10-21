package common

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 定义常量
const (
	NWarehouses    = 10
	NDistricts     = 10
	NCustomers     = 3000
	NCarriers      = 10
	ConsumptionLow = iota
	ConsumptionMedium
	ConsumptionHigh
)

// 事务类型
type TransactionType int

const (
	TransactionNewOrder TransactionType = iota
	TransactionPayment
	TransactionOrderStatus
	TransactionDelivery
	TransactionStockLevel
)

// 依赖类型
//type DependencyType int
//
//const (
//	DependencyWeak DependencyType = iota
//	DependencyStrong
//)

// 通用事务结构
type TPCCTransaction struct {
	mu sync.RWMutex

	// 静态变量
	orderCounters       [10]*atomic.Uint64
	CLasts              [3000]string
	CLastToCID          map[string][]int32
	WDCLatestOrder      map[string]OrderInfo
	WDOldestNewOrder    map[string][]OrderInfo // 使用slice模拟queue
	WDLatestOrderLines  map[string][]OrderLineInfo
	WDOrderLineCounters [NWarehouses][NDistricts]uint64
	OLIIDNum            map[uint64]int

	// 实例变量
	Random          *rand.Rand
	ReadRows        map[string]bool
	UpdateRows      map[string]bool
	Children        []ChildTransaction
	Siblings        []*TPCCTransaction
	ExecutionTime   int
	Type            TransactionType
	CoutConditional *ConditionalOutputStream
}

type ChildTransaction struct {
	Transaction *TPCCTransaction
	Dependency  DependencyType
}

type OrderInfo struct {
	OID    uint64
	OOLCnt uint64
	OCID   int64
}

type OrderLineInfo struct {
	OID      uint64
	OLIID    uint64
	OLNumber int
}

// 全局事务实例
var globalTransaction *TPCCTransaction
var once sync.Once

func GetTPCCTransaction() *TPCCTransaction {
	once.Do(func() {
		globalTransaction = &TPCCTransaction{
			orderCounters:      [10]*atomic.Uint64{},
			CLastToCID:         make(map[string][]int32),
			WDCLatestOrder:     make(map[string]OrderInfo),
			WDOldestNewOrder:   make(map[string][]OrderInfo),
			WDLatestOrderLines: make(map[string][]OrderLineInfo),
			OLIIDNum:           make(map[uint64]int),
			ReadRows:           make(map[string]bool),
			UpdateRows:         make(map[string]bool),
			Children:           make([]ChildTransaction, 0),
			Siblings:           make([]*TPCCTransaction, 0),
			ExecutionTime:      ConsumptionMedium,
			CoutConditional:    NewConditionalOutputStream(),
			Random:             rand.New(rand.NewSource(time.Now().UnixNano())),
		}

		// 初始化order counters
		for i := 0; i < 10; i++ {
			globalTransaction.orderCounters[i] = &atomic.Uint64{}
			globalTransaction.orderCounters[i].Store(1)
		}

		// 初始化CLasts和CLastToCID
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < 3000; i++ {
			var lastName string
			if i < 1000 {
				lastName = randLastName(random, i)
			} else {
				lastName = randLastName(random, nonUniformDistribution(random, 255, 0, 999))
			}
			globalTransaction.CLasts[i] = lastName
			globalTransaction.CLastToCID[lastName] = append(globalTransaction.CLastToCID[lastName], int32(i+1))
		}

		// 初始化WDOrderLineCounters
		for i := 0; i < NWarehouses; i++ {
			for j := 0; j < NDistricts; j++ {
				globalTransaction.WDOrderLineCounters[i][j] = 0
			}
		}
	})
	return globalTransaction
}

func (t *TPCCTransaction) IncrementOrder(idx int) uint64 {
	if idx < 0 || idx >= len(t.orderCounters) {
		return 0
	}
	return t.orderCounters[idx].Add(1) - 1 // 返回增加前的值
}

func (t *TPCCTransaction) GetOrder(idx int) uint64 {
	if idx < 0 || idx >= len(t.orderCounters) {
		return 0
	}
	return t.orderCounters[idx].Load()
}

func (t *TPCCTransaction) GetExecutionTime() int {
	return t.ExecutionTime
}

func (t *TPCCTransaction) SetExecutionTime(time int) {
	t.ExecutionTime = time
}

func (t *TPCCTransaction) AddChild(child *TPCCTransaction, dependency DependencyType) {
	t.Children = append(t.Children, ChildTransaction{
		Transaction: child,
		Dependency:  dependency,
	})
}

func (t *TPCCTransaction) GetChildren() []ChildTransaction {
	return t.Children
}

func (t *TPCCTransaction) AddReadRow(row string) {
	t.ReadRows[row] = true
}

func (t *TPCCTransaction) GetReadRows() map[string]bool {
	return t.ReadRows
}

func (t *TPCCTransaction) AddUpdateRow(row string) {
	t.UpdateRows[row] = true
}

func (t *TPCCTransaction) GetUpdateRows() map[string]bool {
	return t.UpdateRows
}

func (t *TPCCTransaction) AddSibling(sibling *TPCCTransaction) {
	t.Siblings = append(t.Siblings, sibling)
}

func (t *TPCCTransaction) GetSiblings() []*TPCCTransaction {
	return t.Siblings
}

func (t *TPCCTransaction) SetType(transType TransactionType) {
	t.Type = transType
}

func (t *TPCCTransaction) GetType() TransactionType {
	return t.Type
}

func (t *TPCCTransaction) ResetStatic() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.WDCLatestOrder = make(map[string]OrderInfo)
	t.WDOldestNewOrder = make(map[string][]OrderInfo)
	t.WDLatestOrderLines = make(map[string][]OrderLineInfo)

	for i := 0; i < len(t.orderCounters); i++ {
		t.orderCounters[i].Store(1)
	}

	for i := 0; i < NWarehouses; i++ {
		for j := 0; j < NDistricts; j++ {
			t.WDOrderLineCounters[i][j] = 0
		}
	}
}

func (t *TPCCTransaction) PrintCustomerInfo() {
	t.CoutConditional.Print("c_lasts: ")
	for i := 0; i < 3000; i++ {
		t.CoutConditional.Print(t.CLasts[i] + " ")
	}
	t.CoutConditional.Println("")
}

func (t *TPCCTransaction) PrintOrderLineInfo() {
	t.CoutConditional.Println("ol_i_id_num: ")
	for key, value := range t.OLIIDNum {
		t.CoutConditional.Printf("%d:%d\t", key, value)
	}
	t.CoutConditional.Println("")
}

// NewOrder事务
type NewOrderTransaction struct {
	*TPCCTransaction
	WID        uint64
	DID        uint64
	CID        int64
	OOLCnt     uint64
	OrderLines [15]struct {
		OLIID       uint64
		OLSupplyWID uint64
		OLQuantity  uint64
	}
}

func NewNewOrderTransaction(random *rand.Rand) *NewOrderTransaction {
	return &NewOrderTransaction{
		TPCCTransaction: GetTPCCTransaction(),
	}
}

func (not *NewOrderTransaction) MakeNewOrder() *NewOrderTransaction {
	newOrderTx := NewNewOrderTransaction(not.Random)
	newOrderTx.WID = uint64(not.Random.Intn(NWarehouses)) + 1

	newOrderTx.DID = uint64(not.Random.Intn(NDistricts)) + 1
	newOrderTx.CID = int64(nonUniformDistribution(not.Random, 1023, 1, NCustomers))
	newOrderTx.OOLCnt = uint64(not.Random.Intn(11) + 5) // 5-15

	for i := 0; i < int(newOrderTx.OOLCnt); i++ {
		retry := false
		var randomID uint32

		for {
			retry = false
			randomID = uint32(nonUniformDistribution(not.Random, 8191, 1, 100000))

			for k := 0; k < i; k++ {
				if uint64(randomID) == newOrderTx.OrderLines[k].OLIID {
					retry = true
					break
				}
			}

			if !retry {
				break
			}
		}

		newOrderTx.OrderLines[i].OLIID = uint64(randomID)
		newOrderTx.OrderLines[i].OLSupplyWID = newOrderTx.WID
		newOrderTx.OrderLines[i].OLQuantity = uint64(not.Random.Intn(10) + 1)
	}

	return newOrderTx
}

func (not *NewOrderTransaction) MakeTransaction() *TPCCTransaction {

	newOrderTx := not.MakeNewOrder()

	root := GetTPCCTransaction() // 根事务
	root.SetType(TransactionNewOrder)
	root.SetExecutionTime(ConsumptionLow)

	//// warehouse子事务
	wAccess := GetTPCCTransaction()

	wAccess.AddReadRow("Wtax-" + uintToString(newOrderTx.WID))

	// district子事务
	dAccess := GetTPCCTransaction()
	dAccess.AddReadRow("Dtax-" + uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID))
	dAccess.AddUpdateRow("DnextOId-" + uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID))

	nextOID := not.IncrementOrder(int(newOrderTx.DID - 1))

	// newOrder子事务
	noAccess := GetTPCCTransaction()
	noAccess.AddReadRow("NO-" + uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID) + "-" + uintToString(nextOID))
	noAccess.AddUpdateRow("NO-" + uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID))

	// order子事务
	oAccess := GetTPCCTransaction()
	oAccess.AddUpdateRow("O-" + uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID) + "-" + uintToString(nextOID) + "-" + int64ToString(newOrderTx.CID))

	// items子事务
	itemsAccess := GetTPCCTransaction()
	itemsAccess.SetExecutionTime(ConsumptionHigh)

	for i := 0; i < int(newOrderTx.OOLCnt); i++ {
		not.OLIIDNum[newOrderTx.OrderLines[i].OLIID]++

		// item子事务
		iAccess := GetTPCCTransaction()
		iAccess.AddReadRow("I-" + uintToString(newOrderTx.OrderLines[i].OLIID))

		// orderLine子事务
		olAccess := GetTPCCTransaction()
		olAccess.AddReadRow("OL-" + uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID) + "-" + uintToString(nextOID) + "-" + intToString(i))
		olAccess.AddUpdateRow("OL-" + uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID) + "-" + uintToString(nextOID) + "-" + intToString(i))

		// stock子事务
		sAccess := GetTPCCTransaction()
		sAccess.AddReadRow("S-" + uintToString(newOrderTx.OrderLines[i].OLSupplyWID) + "-" + uintToString(newOrderTx.OrderLines[i].OLIID))
		sAccess.AddUpdateRow("S-" + uintToString(newOrderTx.OrderLines[i].OLSupplyWID) + "-" + uintToString(newOrderTx.OrderLines[i].OLIID))

		iAccess.AddChild(sAccess, WEAK)
		iAccess.AddChild(olAccess, WEAK)

		wdKey := uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID)
		wIndex := int(newOrderTx.WID - 1)
		dIndex := int(newOrderTx.DID - 1)

		if not.WDOrderLineCounters[wIndex][dIndex] < 20 {
			not.WDLatestOrderLines[wdKey] = append(not.WDLatestOrderLines[wdKey], OrderLineInfo{
				OID:      nextOID,
				OLIID:    newOrderTx.OrderLines[i].OLIID,
				OLNumber: i,
			})
		} else {
			index := not.WDOrderLineCounters[wIndex][dIndex] % 20
			not.WDLatestOrderLines[wdKey][index] = OrderLineInfo{
				OID:      nextOID,
				OLIID:    newOrderTx.OrderLines[i].OLIID,
				OLNumber: i,
			}
		}

		not.WDOrderLineCounters[wIndex][dIndex]++
		itemsAccess.AddChild(iAccess, STRONG)
	}

	dAccess.AddChild(noAccess, WEAK)
	dAccess.AddChild(oAccess, WEAK)
	dAccess.AddChild(itemsAccess, STRONG)

	// customer子事务
	cAccess := GetTPCCTransaction()
	cAccess.AddReadRow("Cdiscount-" + uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID) + "-" + int64ToString(newOrderTx.CID))

	root.AddChild(wAccess, STRONG)
	root.AddChild(dAccess, STRONG)
	root.AddChild(cAccess, STRONG)

	wdcKey := uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID) + "-" + int64ToString(newOrderTx.CID)
	not.WDCLatestOrder[wdcKey] = OrderInfo{
		OID:    nextOID,
		OOLCnt: newOrderTx.OOLCnt,
		OCID:   newOrderTx.CID,
	}

	wdKey := uintToString(newOrderTx.WID) + "-" + uintToString(newOrderTx.DID)
	not.WDOldestNewOrder[wdKey] = append(not.WDOldestNewOrder[wdKey], OrderInfo{
		OID:    nextOID,
		OOLCnt: newOrderTx.OOLCnt,
		OCID:   newOrderTx.CID,
	})

	return root
}

// Payment事务
type PaymentTransaction struct {
	*TPCCTransaction
	WID     uint64
	DID     uint64
	CID     int64
	CLast   string
	HAmount uint64
}

func NewPaymentTransaction(random *rand.Rand) *PaymentTransaction {
	return &PaymentTransaction{
		TPCCTransaction: GetTPCCTransaction(),
	}
}

func (pt *PaymentTransaction) MakePayment() *PaymentTransaction {
	paymentTx := NewPaymentTransaction(pt.Random)
	paymentTx.WID = uint64(pt.Random.Intn(NWarehouses)) + 1
	paymentTx.DID = uint64(pt.Random.Intn(NDistricts)) + 1

	y := pt.Random.Intn(100) + 1
	if y <= 60 {
		for {
			paymentTx.CLast = randLastName(pt.Random, nonUniformDistribution(pt.Random, 255, 0, 999))
			if len(pt.CLastToCID[paymentTx.CLast]) > 0 {
				break
			}
		}
		paymentTx.CID = -1
	} else {
		paymentTx.CID = int64(nonUniformDistribution(pt.Random, 1023, 1, NCustomers))
	}

	paymentTx.HAmount = uint64(pt.Random.Intn(5000) + 1)
	return paymentTx
}

func (pt *PaymentTransaction) MakeTransaction() *TPCCTransaction {
	paymentTx := pt.MakePayment()

	root := GetTPCCTransaction()
	root.SetType(TransactionPayment)
	root.SetExecutionTime(ConsumptionLow)

	// warehouse子事务
	wAccess := GetTPCCTransaction()

	// district子事务
	dAccess := GetTPCCTransaction()
	dAccess.AddReadRow("Dytd-" + uintToString(paymentTx.WID) + "-" + uintToString(paymentTx.DID))
	dAccess.AddUpdateRow("Dytd-" + uintToString(paymentTx.WID) + "-" + uintToString(paymentTx.DID))

	// history子事务
	hAccess := GetTPCCTransaction()

	// customer子事务
	cAccess := GetTPCCTransaction()
	if paymentTx.CID == -1 {
		cIDs := pt.CLastToCID[paymentTx.CLast]
		for _, cID := range cIDs {
			cAccess.AddReadRow("Cbalance-" + uintToString(paymentTx.WID) + "-" + uintToString(paymentTx.DID) + "-" + int32ToString(cID))
		}

		paymentTx.CID = int64(cIDs[(len(cIDs)-1)/2])
		cAccess.SetExecutionTime(ConsumptionHigh)
		cAccess.AddChild(hAccess, WEAK)
	} else {
		cAccess.AddReadRow("Cbalance-" + uintToString(paymentTx.WID) + "-" + uintToString(paymentTx.DID) + "-" + int64ToString(paymentTx.CID))
		root.AddChild(hAccess, WEAK)
	}

	cAccess.AddUpdateRow("C-" + uintToString(paymentTx.WID) + "-" + uintToString(paymentTx.DID) + "-" + int64ToString(paymentTx.CID))
	hAccess.AddUpdateRow("H-" + uintToString(paymentTx.WID) + "-" + uintToString(paymentTx.DID) + "-" + int64ToString(paymentTx.CID))

	root.AddChild(wAccess, WEAK)
	root.AddChild(dAccess, WEAK)
	root.AddChild(cAccess, WEAK)

	return root
}

// OrderStatus事务
type OrderStatusTransaction struct {
	*TPCCTransaction
	WID   uint64
	DID   uint64
	CID   int64
	CLast string
}

func NewOrderStatusTransaction(random *rand.Rand) *OrderStatusTransaction {
	return &OrderStatusTransaction{
		TPCCTransaction: GetTPCCTransaction(),
	}
}

func (ost *OrderStatusTransaction) MakeOrderStatus() *OrderStatusTransaction {
	orderStatusTx := NewOrderStatusTransaction(ost.Random)
	var wdcKey string

	for {
		orderStatusTx.WID = uint64(ost.Random.Intn(NWarehouses)) + 1
		orderStatusTx.DID = uint64(ost.Random.Intn(NDistricts)) + 1

		y := ost.Random.Intn(100) + 1
		var tempCID int32

		if y <= 60 {
			for {
				orderStatusTx.CLast = randLastName(ost.Random, nonUniformDistribution(ost.Random, 255, 0, 999))
				if len(ost.CLastToCID[orderStatusTx.CLast]) > 0 {
					break
				}
			}
			orderStatusTx.CID = -1
			cIDs := ost.CLastToCID[orderStatusTx.CLast]
			tempCID = cIDs[(len(cIDs)-1)/2]
		} else {
			orderStatusTx.CID = int64(nonUniformDistribution(ost.Random, 1023, 1, NCustomers))
			orderStatusTx.CLast = ""
			tempCID = int32(orderStatusTx.CID)
		}

		wdcKey = uintToString(orderStatusTx.WID) + "-" + uintToString(orderStatusTx.DID) + "-" + int32ToString(tempCID)
		if ost.WDCLatestOrder[wdcKey].OID != 0 {
			break
		}
	}

	return orderStatusTx
}

func (ost *OrderStatusTransaction) MakeTransaction() *TPCCTransaction {
	orderStatusTx := ost.MakeOrderStatus()

	var root *TPCCTransaction
	cAccess := GetTPCCTransaction()
	oAccess := GetTPCCTransaction()
	oAccess.SetExecutionTime(ConsumptionHigh)

	if orderStatusTx.CID == -1 {
		cIDs := ost.CLastToCID[orderStatusTx.CLast]
		for _, cID := range cIDs {
			cAccess.AddReadRow("C-" + uintToString(orderStatusTx.WID) + "-" + uintToString(orderStatusTx.DID) + "-" + int32ToString(cID))
		}

		orderStatusTx.CID = int64(cIDs[(len(cIDs)-1)/2])
		cAccess.SetExecutionTime(ConsumptionHigh)
		cAccess.AddChild(oAccess, WEAK)
		root = cAccess
	} else {
		cAccess.AddReadRow("C-" + uintToString(orderStatusTx.WID) + "-" + uintToString(orderStatusTx.DID) + "-" + int64ToString(orderStatusTx.CID))
		root = GetTPCCTransaction()
		root.SetExecutionTime(ConsumptionLow)
		root.AddChild(cAccess, WEAK)
		root.AddChild(oAccess, WEAK)
	}

	wdcKey := uintToString(orderStatusTx.WID) + "-" + uintToString(orderStatusTx.DID) + "-" + int64ToString(orderStatusTx.CID)
	if _, exists := ost.WDCLatestOrder[wdcKey]; !exists {
		return root
	}

	latestOrder := ost.WDCLatestOrder[wdcKey]
	oAccess.AddReadRow("O-" + wdcKey)

	for i := 0; i < int(latestOrder.OOLCnt); i++ {
		olAccess := GetTPCCTransaction()
		olAccess.AddReadRow("OL-" + uintToString(orderStatusTx.WID) + "-" + uintToString(orderStatusTx.DID) + "-" + uintToString(latestOrder.OID))
		oAccess.AddChild(olAccess, WEAK)
	}

	root.SetType(TransactionOrderStatus)
	return root
}

// Delivery事务
type DeliveryTransaction struct {
	*TPCCTransaction
	WID         uint64
	OCarrierID  uint64
	OLDeliveryD int64
}

func NewDeliveryTransaction(random *rand.Rand) *DeliveryTransaction {
	return &DeliveryTransaction{
		TPCCTransaction: GetTPCCTransaction(),
	}
}

func (dt *DeliveryTransaction) MakeDelivery() *DeliveryTransaction {
	deliveryTx := NewDeliveryTransaction(dt.Random)
	deliveryTx.WID = uint64(dt.Random.Intn(NWarehouses)) + 1
	deliveryTx.OCarrierID = uint64(dt.Random.Intn(NCarriers)) + 1
	deliveryTx.OLDeliveryD = time.Now().Unix()
	return deliveryTx
}

func (dt *DeliveryTransaction) MakeTransaction() *TPCCTransaction {
	deliveryTx := dt.MakeDelivery()

	root := GetTPCCTransaction()
	root.SetType(TransactionDelivery)
	root.SetExecutionTime(ConsumptionHigh)

	for i := 1; i <= NDistricts; i++ {
		noCAccess := GetTPCCTransaction()

		wdKey := uintToString(deliveryTx.WID) + "-" + intToString(i)
		if len(dt.WDOldestNewOrder[wdKey]) == 0 {
			continue
		}

		oldestNewOrder := dt.WDOldestNewOrder[wdKey][0]
		dt.WDOldestNewOrder[wdKey] = dt.WDOldestNewOrder[wdKey][1:]

		noCAccess.AddUpdateRow("NO-" + wdKey + "-" + uintToString(oldestNewOrder.OID))
		noCAccess.AddUpdateRow("NO-" + wdKey)
		noCAccess.SetExecutionTime(ConsumptionHigh)

		oAccess := GetTPCCTransaction()
		oAccess.AddReadRow("O-" + wdKey + "-" + uintToString(oldestNewOrder.OID) + "-" + int64ToString(oldestNewOrder.OCID))
		oAccess.AddUpdateRow("O-" + wdKey + "-" + uintToString(oldestNewOrder.OID) + "-" + int64ToString(oldestNewOrder.OCID))

		olsAccess := GetTPCCTransaction()
		for j := 0; j < int(oldestNewOrder.OOLCnt); j++ {
			olAccess := GetTPCCTransaction()
			olAccess.AddReadRow("OLdelivery-" + wdKey + "-" + uintToString(oldestNewOrder.OID) + "-" + intToString(j))
			olsAccess.AddChild(olAccess, STRONG)
		}
		olsAccess.AddUpdateRow("OLdelivery-" + wdKey)

		noCAccess.AddChild(oAccess, STRONG)
		noCAccess.AddChild(olsAccess, STRONG)

		noCAccess.AddReadRow("C-" + wdKey + "-" + int64ToString(oldestNewOrder.OCID))
		noCAccess.AddUpdateRow("Cbalance-" + wdKey + "-" + int64ToString(oldestNewOrder.OCID))
		noCAccess.AddReadRow("Cdelivery-" + wdKey + "-" + int64ToString(oldestNewOrder.OCID))
		noCAccess.AddUpdateRow("Cdelivery-" + wdKey + "-" + int64ToString(oldestNewOrder.OCID))

		root.AddChild(noCAccess, WEAK)
	}

	return root
}

// StockLevel事务
type StockLevelTransaction struct {
	*TPCCTransaction
	WID       uint64
	DID       uint64
	Threshold uint64
}

func NewStockLevelTransaction(random *rand.Rand) *StockLevelTransaction {
	return &StockLevelTransaction{
		TPCCTransaction: GetTPCCTransaction(),
	}
}

func (slt *StockLevelTransaction) MakeStockLevel() *StockLevelTransaction {
	stockLevelTx := NewStockLevelTransaction(slt.Random)

	for {
		stockLevelTx.WID = uint64(slt.Random.Intn(NWarehouses)) + 1
		stockLevelTx.DID = uint64(slt.Random.Intn(NDistricts)) + 1
		wdKey := uintToString(stockLevelTx.WID) + "-" + uintToString(stockLevelTx.DID)
		if _, exists := slt.WDLatestOrderLines[wdKey]; exists {
			break
		}
	}

	stockLevelTx.Threshold = uint64(slt.Random.Intn(11) + 10)
	return stockLevelTx
}

func (slt *StockLevelTransaction) MakeTransaction() *TPCCTransaction {
	stockLevelTx := slt.MakeStockLevel()

	dAccess := GetTPCCTransaction()
	dAccess.SetType(TransactionStockLevel)
	dAccess.AddReadRow("D-" + uintToString(stockLevelTx.WID) + "-" + uintToString(stockLevelTx.DID))

	//nextOID := slt.GetOrder(int(stockLevelTx.DID - 1))

	olAccess := GetTPCCTransaction()
	wdKey := uintToString(stockLevelTx.WID) + "-" + uintToString(stockLevelTx.DID)

	latestOrderLines, exists := slt.WDLatestOrderLines[wdKey]
	if !exists {
		return nil
	}

	for _, orderLine := range latestOrderLines {
		sAccess := GetTPCCTransaction()
		sAccess.AddReadRow("Sqtys-" + uintToString(stockLevelTx.WID) + "-" + uintToString(orderLine.OLIID))
		olAccess.AddChild(sAccess, WEAK)
	}

	olAccess.AddReadRow("OL-" + wdKey)
	olAccess.SetExecutionTime(ConsumptionHigh)
	dAccess.AddChild(olAccess, WEAK)

	return dAccess
}

// 辅助函数
func nonUniformDistribution(random *rand.Rand, A int, min int, max int) int {
	return ((random.Intn(A) | random.Intn(A)) % (max - min + 1)) + min
}

func randLastName(random *rand.Rand, num int) string {
	// 简化实现，实际应用中应该使用真实的姓氏列表
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	return lastNames[num%len(lastNames)]
}

func uintToString(n uint64) string {
	// 简化实现
	return string(rune(n + '0'))
}

func intToString(n int) string {
	// 简化实现
	return string(rune(n + '0'))
}

func int32ToString(n int32) string {
	// 简化实现
	return string(rune(n + '0'))
}

func int64ToString(n int64) string {
	// 简化实现
	return string(rune(n + '0'))
}

// ConditionalOutputStream模拟
type ConditionalOutputStream struct{}

func NewConditionalOutputStream() *ConditionalOutputStream {
	return &ConditionalOutputStream{}
}

func (cos *ConditionalOutputStream) Print(msg string) {
	// 简化实现
	print(msg)
}

func (cos *ConditionalOutputStream) Println(msg string) {
	// 简化实现
	println(msg)
}

func (cos *ConditionalOutputStream) Printf(format string, args ...interface{}) {
	// 简化实现
	printf(format, args...)
}

// 简化版的printf实现
func printf(format string, args ...interface{}) {
	// 实际实现应该处理格式化
	print(format)
}
