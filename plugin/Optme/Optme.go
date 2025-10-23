package Optme

import "Janus/plugin/Common"

type OptME struct {
	statistics Statistics
	blocks     []*Common.Block
	batches    [][]*OptmeTransaction
	acgs       []*AddressBasedConflictGraph
}
