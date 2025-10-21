package common

// TransactionType represents the type of transaction
//type TransactionType int

const (
	NEW_ORDER TransactionType = iota
	PAYMENT
	ORDER_STATUS
	DELIVERY
	STOCK_LEVEL
)

// ConsumptionType represents the consumption level (High, Medium, Low)
type ConsumptionType int

const (
	HIGH   ConsumptionType = 80
	MEDIUM ConsumptionType = 40
	LOW    ConsumptionType = 20
)

// N_WAREHOUSES holds the number of warehouses, to be set elsewhere
var N_WAREHOUSES int

// N_DISTRICTS is a constant value representing the number of districts
const N_DISTRICTS = 10

// N_CUSTOMERS is a constant value representing the number of customers
const N_CUSTOMERS = 3000

// N_CARRIERS is a constant value representing the number of carriers
const N_CARRIERS = 10

// transactionTypeToString converts the transaction type to a string
func transactionTypeToString(t TransactionType) string {
	switch t {
	case NEW_ORDER:
		return "NEW_ORDER"
	case PAYMENT:
		return "PAYMENT"
	case ORDER_STATUS:
		return "ORDER_STATUS"
	case DELIVERY:
		return "DELIVERY"
	case STOCK_LEVEL:
		return "STOCK_LEVEL"
	default:
		return "UNKNOWN"
	}
}
