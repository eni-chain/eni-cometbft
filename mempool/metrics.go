package mempool

import (
	"github.com/go-kit/kit/metrics"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsSubsystem = "mempool"
)

//go:generate go run ../scripts/metricsgen -struct=Metrics

// Metrics contains metrics exposed by this package.
// see MetricsProvider for descriptions.
type Metrics struct {
	// Number of uncommitted transactions in the mempool.
	Size metrics.Gauge

	// Total size of the mempool in bytes.
	SizeBytes metrics.Gauge

	// Histogram of transaction sizes in bytes.
	TxSizeBytes metrics.Histogram `metrics_buckettype:"exp" metrics_bucketsizes:"1,3,7"`

	// Accumulated transaction sizes in bytes.
	AllTxSizeBytes metrics.Counter
	// FailedTxs defines the number of failed transactions. These are
	// transactions that failed to make it into the mempool because they were
	// deemed invalid.
	// metrics:Number of failed transactions.
	FailedTxs metrics.Counter

	// RejectedTxs defines the number of rejected transactions. These are
	// transactions that failed to make it into the mempool due to resource
	// limits, e.g. mempool is full.
	// metrics:Number of rejected transactions.
	RejectedTxs metrics.Counter

	// EvictedTxs defines the number of evicted transactions. These are valid
	// transactions that passed CheckTx and make it into the mempool but later
	// became invalid.
	// metrics:Number of evicted transactions.
	EvictedTxs metrics.Counter

	// Number of times transactions are rechecked in the mempool.
	RecheckTimes metrics.Counter

	// Number of connections being actively used for gossiping transactions
	// (experimental feature).
	ActiveOutboundConnections metrics.Gauge

	// added for eni
	// Number of pending transactions in mempool
	PendingSize metrics.Gauge

	// ExpiredTxs defines the number of expired transactions. These are valid
	// transactions that passed CheckTx and existed in the mempool but were not
	// get picked up in time and eventually got expired and removed from mempool
	//metrics:Number of expired transactions.
	ExpiredTxs metrics.Counter

	// Number of removed tx from mempool
	RemovedTxs metrics.Counter

	// Number of txs inserted to mempool
	InsertedTxs metrics.Counter
}
