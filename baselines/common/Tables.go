package common

// Warehouse 仓库表
type Warehouse struct {
	WID  uint32 // 仓库编号 (w_id)
	WYTD uint64 // 仓库总收入 (w_ytd)
}

// District 区域表
type District struct {
	DID      uint32 // 区域编号 (d_id)
	DWID     uint32 // 仓库编号 (d_w_id)
	DYTD     uint64 // 区域总收入 (d_ytd)
	DNextOID uint64 // 下一个订单编号 (d_next_o_id)
}

// Customer 客户表
type Customer struct {
	CID      int32  // 客户编号 (c_id)
	CDID     uint32 // 区域编号 (c_d_id)
	CWID     uint32 // 仓库编号 (c_w_id)
	CBalance uint64 // 余额 (c_balance)
	CLast    string // 姓氏 (c_last)
}

// History 历史支付记录表
type History struct {
	HCID    int32  // 客户编号 (h_c_id)
	HCDID   uint32 // 客户所在区域编号 (h_c_d_id)
	HCWID   uint32 // 客户所在仓库编号 (h_c_w_id)
	HDID    uint32 // 区域编号 (h_d_id)
	HWID    uint32 // 仓库编号 (h_w_id)
	HAmount uint32 // 支付金额 (h_amount)
}

// NewOrder 新订单表
type NewOrder struct {
	NoOID uint64 // 订单编号 (no_o_id)
	NoDID uint32 // 区域编号 (no_d_id)
	NoWID uint32 // 仓库编号 (no_w_id)
}

// Order 订单表
type Order struct {
	OID        uint64 // 订单编号 (o_id)
	ODID       uint32 // 区域编号 (o_d_id)
	OWID       uint32 // 仓库编号 (o_w_id)
	OCID       int32  // 客户编号 (o_c_id)
	OEntryD    uint64 // 订单日期 (o_entry_d)
	OCarrierID uint32 // 承运人编号 (o_carrier_id)
	OOlCnt     uint32 // 订单中商品数量 (o_ol_cnt)
}

// OrderLine 订单明细表
type OrderLine struct {
	OlOID       uint64 // 订单编号 (ol_o_id)
	OlDID       uint32 // 区域编号 (ol_d_id)
	OlWID       uint32 // 仓库编号 (ol_w_id)
	OlNumber    uint32 // 订单行号 (ol_number)
	OlIID       uint32 // 商品编号 (ol_i_id)
	OlSupplyWID uint32 // 供应仓库编号 (ol_supply_w_id)
	OlDeliveryD uint64 // 交货日期 (ol_delivery_d)
	OlQuantity  uint32 // 商品数量 (ol_quantity)
	OlAmount    uint32 // 商品金额 (ol_amount)
}

// Item 商品表
type Item struct {
	IID    uint32 // 商品编号 (i_id)
	IPrice uint32 // 商品价格 (i_price)
}

// Stock 库存表
type Stock struct {
	SIID      uint32 // 商品编号 (s_i_id)
	SWID      uint32 // 仓库编号 (s_w_id)
	SQuantity uint32 // 商品数量 (s_quantity)
	SYTD      uint64 // 商品总销售数量 (s_ytd)
	SOrderCnt uint32 // 商品订单数量 (s_order_cnt)
}
