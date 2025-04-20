package mempool

import (
	"context"
	"errors"
	"fmt"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/clist"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/libs/log"
)

var _ Mempool = (*FastTxMempool)(nil)

type FastTxMempool struct {
	logger       log.Logger
	metrics      *Metrics
	config       *config.MempoolConfig
	proxyAppConn proxy.AppConnMempool

	// txsAvailable fires once for each height when the mempool is not empty
	txsAvailable         chan struct{}
	notifiedTxsAvailable bool

	// height defines the last block height process during Update()
	height int64

	// sizeBytes defines the total size of the mempool (sum of all tx bytes)
	sizeBytes int64

	totalTxCnt int64

	TxQueues []*FastTxQueue

	// cache defines a fixed-size cache of already seen transactions as this
	// reduces pressure on the proxyApp.
	cache TxCache

	// txStore defines the main storage of valid transactions. Indexes are built
	// on top of this store.
	txStore *TxStore

	// gossipIndex defines the gossiping index of valid transactions via a
	// thread-safe linked-list. We also use the gossip index as a cursor for
	// rechecking transactions already in the mempool.
	gossipIndex *clist.CList

	// recheckCursor and recheckEnd are used as cursors based on the gossip index
	// to recheck transactions that are already in the mempool. Iteration is not
	// thread-safe and transaction may be mutated in serial order.
	//
	// XXX/TODO: It might be somewhat of a codesmell to use the gossip index for
	// iterator and cursor management when rechecking transactions. If the gossip
	// index changes or is removed in a future refactor, this will have to be
	// refactored. Instead, we should consider just keeping a slice of a snapshot
	// of the mempool's current transactions during Update and an integer cursor
	// into that slice. This, however, requires additional O(n) space complexity.
	recheckCursor *clist.CElement // next expected response
	recheckEnd    *clist.CElement // re-checking stops here

	// A read/write lock is used to safe guard updates, insertions and deletions
	// from the mempool. A read-lock is implicitly acquired when executing CheckTx,
	// however, a caller must explicitly grab a write-lock via Lock when updating
	// the mempool via Update().
	mtx       sync.RWMutex
	preCheck  PreCheckFunc
	postCheck PostCheckFunc

	// NodeID to count of transactions failing CheckTx
	failedCheckTxCounts    map[p2p.ID]uint64
	mtxFailedCheckTxCounts sync.RWMutex

	peerManager PeerEvictor
}

func (txmp *FastTxMempool) WaitForNextTx() <-chan struct{} {
	return txmp.gossipIndex.WaitChan()
}

func (txmp *FastTxMempool) NextGossipTx() *clist.CElement {
	return txmp.gossipIndex.Front()
}

// TxMempoolOption sets an optional parameter on the TxMempool.
type TxFastMempoolOption func(*FastTxMempool)

func NewFastTxMempool(
	logger log.Logger,
	cfg *config.MempoolConfig,
	proxyAppConn proxy.AppConnMempool,
	peerManager PeerEvictor,
	options ...TxFastMempoolOption,
) *FastTxMempool {
	numCPU := runtime.NumCPU()
	txmp := &FastTxMempool{
		logger:              logger,
		config:              cfg,
		proxyAppConn:        proxyAppConn,
		height:              -1,
		TxQueues:            make([]*FastTxQueue, numCPU*2),
		cache:               NopTxCache{},
		metrics:             NopMetrics(),
		txStore:             NewTxStore(),
		gossipIndex:         clist.New(),
		failedCheckTxCounts: map[p2p.ID]uint64{},
		peerManager:         peerManager,
	}

	if cfg.CacheSize > 0 {
		txmp.cache = NewLRUTxCache(cfg.CacheSize)
	}

	for i := 0; i < len(txmp.TxQueues); i++ {
		txmp.TxQueues[i] = newFastTxQueue(cfg)
	}

	for _, opt := range options {
		opt(txmp)
	}

	return txmp
}

// WithPreCheck sets a filter for the mempool to reject a transaction if txmp(tx)
// returns an error. This is executed before CheckTx. It only applies to the
// first created block. After that, Update() overwrites the existing value.
func WithPreCheckTxFastMpool(f PreCheckFunc) TxFastMempoolOption {
	return func(txmp *FastTxMempool) { txmp.preCheck = f }
}

// WithPostCheck sets a filter for the mempool to reject a transaction if
// txmp(tx, resp) returns an error. This is executed after CheckTx. It only applies
// to the first created block. After that, Update overwrites the existing value.
func WithPostCheckTxFastMpool(f PostCheckFunc) TxFastMempoolOption {
	return func(txmp *FastTxMempool) { txmp.postCheck = f }
}

// WithMetrics sets the mempool's metrics collector.
func WithMetricsTxFastMpool(metrics *Metrics) TxFastMempoolOption {
	return func(txmp *FastTxMempool) { txmp.metrics = metrics }
}

func (txmp *FastTxMempool) TxStore() *TxStore {
	return txmp.txStore
}

func (txmp *FastTxMempool) CheckTx(tx types.Tx, callback func(*abci.ResponseCheckTx), txInfo TxInfo) error {
	//startTime := time.Now()
	txmp.mtx.RLock()
	defer txmp.mtx.RUnlock()

	if txSize := len(tx); txSize > txmp.config.MaxTxBytes {
		return ErrTxTooLarge{
			Max:    txmp.config.MaxTxBytes,
			Actual: txSize,
		}
	}

	if txmp.preCheck != nil {
		if err := txmp.preCheck(tx); err != nil {
			return ErrPreCheck{Err: err}
		}
	}

	if err := txmp.proxyAppConn.Error(); err != nil {
		return err
	}

	txHash := tx.Key()

	// We add the transaction to the mempool's cache and if the
	// transaction is already present in the cache, i.e. false is returned, then we
	// check if we've seen this transaction and error if we have.
	if !txmp.cache.Push(tx) {
		txmp.txStore.GetOrSetPeerByTxHash(txHash, txInfo.SenderID)
		return ErrTxInCache
	}
	//txmp.logger.Info("CheckTx  Push", "elapsedTime", time.Since(startTime).Microseconds())

	//checkStartTime := time.Now()
	res, err := txmp.proxyAppConn.CheckTx(context.TODO(), &abci.RequestCheckTx{Tx: tx})
	//txmp.logger.Info("proxyAppConn.CheckTx ", "elapsedTime", time.Since(checkStartTime).Microseconds())
	// when a transaction is removed/expired/rejected, this should be called
	// The expire tx handler unreserves the pending nonce
	removeHandler := func(removeFromCache bool) {
		if removeFromCache {
			txmp.cache.Remove(tx)
		}
		if res.ExpireTxHandler != nil {
			res.ExpireTxHandler()
		}
	}

	if err != nil {
		removeHandler(true)
		res.Log = txmp.AppendCheckTxErr(res.Log, err.Error())
	}

	wtx := &WrappedTx{
		tx:                   tx,
		hash:                 txHash,
		timestamp:            time.Now().UTC(),
		height:               txmp.height,
		evmNonce:             res.EVMNonce,
		evmAddress:           res.EVMSenderAddress,
		isEVM:                res.IsEVM,
		removeHandler:        removeHandler,
		isPendingTransaction: res.IsPendingTransaction,
	}

	if err == nil {
		// Concurrent mode, can directly added
		err = txmp.addNewTransaction(wtx, res.ResponseCheckTx, txInfo)
		if err != nil {
			txmp.logger.Info("addNewTransaction failed ", "error", err.Error())
			return err
		}
	}

	if callback != nil {
		callback(res.ResponseCheckTx)
	}
	//txmp.logger.Info("checkTx ", "elapsedTime", time.Since(startTime).Microseconds(), "start time", startTime.Format(time.StampMicro))
	if atomic.LoadInt64(&txmp.totalTxCnt) == 10000 {
		txmp.logger.Info("mempool finish 1w tx save ", "now time", time.Now().Format(time.StampMicro))
	}
	return nil
}

func (txmp *FastTxMempool) RemoveTxByKey(txKey types.TxKey) error {
	txmp.Lock()
	defer txmp.Unlock()

	// remove the committed transaction from the transaction store and indexes
	if wtx := txmp.txStore.GetTxByHash(txKey); wtx != nil {
		txmp.removeTx(wtx, false, true, true)
		return nil
	}
	// todo remove tx
	return errors.New("transaction not found")
}

func (txmp *FastTxMempool) removeTx(wtx *WrappedTx, removeFromCache bool, shouldReenqueue bool, updatePriorityIndex bool) {

	// remove the committed transaction from the transaction store and indexes
	if wtx := txmp.txStore.GetTxByHash(wtx.hash); wtx != nil {
		txmp.removeTx(wtx, false, true, true)

	}

	queueIdx := txmp.DistQueueIdx(wtx.evmAddress)
	txmp.TxQueues[queueIdx].DelTx(txmp, wtx, false, false, true)
	return
}

func (txmp *FastTxMempool) ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Txs {
	txmp.Lock()
	defer txmp.Unlock()
	var (
		totalGas        int64
		totalSize       int64
		nonzeroGasTxCnt int64
	)
	startTime := time.Now()
	var txs []types.Tx

	txIsFetchTotal := 0
	txIsPendingTotal := 0
	//txsTable := make([][]types.Tx, len(txmp.TxQueues))
	for i := 0; i < len(txmp.TxQueues); i++ {
		for _, queue := range txmp.TxQueues[i].evmTx {
			txIsFetchTotal += len(queue.isFetch)
			txIsPendingTotal += len(queue.pendingTxs)
		}

		txmp.TxQueues[i].ForEachTx(func(wtx *WrappedTx) bool {
			size := types.ComputeProtoSizeForTxs([]types.Tx{wtx.tx})

			if maxBytes > -1 && totalSize+size > maxBytes {
				return false
			}
			totalSize += size
			gas := totalGas + wtx.gasWanted
			//if nonzeroGasTxCnt >= minTxsInBlock && maxGas > -1 && gas > maxGas {
			//	return false
			//}

			if len(txs) > 10000 {
				return false
			}

			if maxGas > -1 && gas > maxGas {
				return false
			}
			totalGas = gas

			txs = append(txs, wtx.tx)
			//txs = append(txsTable[i], wtx.tx)
			if wtx.gasWanted > 0 {
				nonzeroGasTxCnt++
			}
			return true
		})
	}
	txmp.logger.Info("ReapMaxBytesMaxGas end", "elapsedTime", time.Since(startTime).Microseconds(), "tx len", len(txs), "totalTxCnt", atomic.LoadInt64(&txmp.totalTxCnt),
		"start time", startTime.Format(time.StampMicro), "txIsPendingTotal len", txIsPendingTotal, "txIsFetchTotal len", txIsFetchTotal)
	return txs
}

func (txmp *FastTxMempool) ReapMaxTxs(max int) types.Txs {
	//TODO implement me
	panic("implement me")
}

// Lock obtains a write-lock on the mempool. A caller must be sure to explicitly
// release the lock when finished.
func (txmp *FastTxMempool) Lock() {
	txmp.mtx.Lock()
}

// Unlock releases a write-lock on the mempool.
func (txmp *FastTxMempool) Unlock() {
	txmp.mtx.Unlock()
}

func (txmp *FastTxMempool) Update(blockHeight int64, blockTxs types.Txs, execTxResult []*abci.ExecTxResult, newPreFn PreCheckFunc, newPostFn PostCheckFunc) error {
	txmp.height = blockHeight
	txmp.notifiedTxsAvailable = false
	startTime := time.Now()
	if newPreFn != nil {
		txmp.preCheck = newPreFn
	}
	if newPostFn != nil {
		txmp.postCheck = newPostFn
	}

	for i, tx := range blockTxs {
		if execTxResult[i].Code == abci.CodeTypeOK {
			// add the valid committed transaction to the cache (if missing)
			_ = txmp.cache.Push(tx)
		}

		// remove the committed transaction from the transaction store and indexes
		if wtx := txmp.txStore.GetTxByHash(tx.Key()); wtx != nil {
			//txmp.removeTx(wtx, false, false, true)
			queueIdx := txmp.DistQueueIdx(wtx.evmAddress)
			txmp.TxQueues[queueIdx].DelTx(txmp, wtx, false, false, true)
		}
	}

	if txmp.Size() > 0 {
		txmp.notifyTxsAvailable()
	}

	txmp.metrics.Size.Set(float64(txmp.Size()))
	txmp.metrics.SizeBytes.Set(float64(txmp.SizeBytes()))
	txmp.logger.Info("Update fastMempool end", "elapsedTime", time.Since(startTime).Microseconds(), "block height", blockHeight, "tx len", blockTxs.Len(), "start time", startTime.Format(time.StampMicro))

	return nil
}

func (txmp *FastTxMempool) FlushAppConn() error {
	return txmp.proxyAppConn.Flush(context.TODO())
}

func (txmp *FastTxMempool) Flush() {
	//TODO implement me
	panic("implement me")
}

func (txmp *FastTxMempool) EnableTxsAvailable() {
	txmp.mtx.Lock()
	defer txmp.mtx.Unlock()

	txmp.txsAvailable = make(chan struct{}, 1)
}

func (txmp *FastTxMempool) Size() int {
	return int(atomic.LoadInt64(&txmp.totalTxCnt))
}

func (txmp *FastTxMempool) SizeBytes() int64 {
	return atomic.LoadInt64(&txmp.sizeBytes)
}

// SetLogger sets the Logger.
func (txmp *FastTxMempool) SetLogger(l log.Logger) {
	txmp.logger = l
}

func (txmp *FastTxMempool) addNewTransaction(wtx *WrappedTx, res *abci.ResponseCheckTx, txInfo TxInfo) error {
	var err error
	if txmp.postCheck != nil {
		err = txmp.postCheck(wtx.tx, res)
	}

	if err != nil || res.Code != abci.CodeTypeOK {
		// ignore bad transactions
		txmp.logger.Info(
			"rejected bad transaction",
			"priority", wtx.priority,
			"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
			"peer_id", txInfo.SenderP2PID,
			"code", res.Code,
			"post_check_err", err,
		)

		txmp.metrics.FailedTxs.Add(1)

		wtx.removeHandler(!txmp.config.KeepInvalidTxsInCache)
		if res.Code != abci.CodeTypeOK {
			txmp.mtxFailedCheckTxCounts.Lock()
			defer txmp.mtxFailedCheckTxCounts.Unlock()
			txmp.failedCheckTxCounts[txInfo.SenderP2PID]++
			if txmp.config.CheckTxErrorBlacklistEnabled && txmp.failedCheckTxCounts[txInfo.SenderP2PID] > uint64(txmp.config.CheckTxErrorThreshold) {
				// evict peer
				if txmp.peerManager != nil {
					txmp.peerManager.Errored(txInfo.SenderP2PID, errors.New("checkTx error exceeded threshold"))
				}
			}
		}
		if err == nil {
			return errors.New(res.Log)
		}
		return err
	}

	//sender := res.Sender
	sender := ""
	//priority := res.Priority
	priority := int64(0)
	//todo:Because the sender is empty, so this is a dead code, comment first, and then optimize later
	//if len(sender) > 0 {
	//	if wtx := txmp.txStore.GetTxBySender(sender); wtx != nil {
	//		txmp.logger.Error(
	//			"rejected incoming good transaction; tx already exists for sender",
	//			"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
	//			"sender", sender,
	//		)
	//		txmp.metrics.RejectedTxs.Add(1)
	//		return nil
	//	}
	//}

	if err := txmp.canAddTx(wtx); err != nil {
		return nil
		//todo: Because the priority is 0, evictTxs is also empty, so here is a dead code, comment first, and then optimize later
		//evictTxs := txmp.priorityIndex.GetEvictableTxs(
		//	priority,
		//	int64(wtx.Size()),
		//	txmp.SizeBytes(),
		//	txmp.config.MaxTxsBytes,
		//)
		//if len(evictTxs) == 0 {
		//	// No room for the new incoming transaction so we just remove it from
		//	// the cache.
		//	wtx.removeHandler(true)
		//	txmp.logger.Error(
		//		"rejected incoming good transaction; mempool full",
		//		"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
		//		"err", err.Error(),
		//	)
		//	txmp.metrics.RejectedTxs.Add(1)
		//	return nil
		//}

		// evict an existing transaction(s)
		//
		// NOTE:
		// - The transaction, toEvict, can be removed while a concurrent
		//   reCheckTx callback is being executed for the same transaction.
		//for _, toEvict := range evictTxs {
		//	txmp.removeTx(toEvict, true, true, true)
		//	txmp.logger.Debug(
		//		"evicted existing good transaction; mempool full",
		//		"old_tx", fmt.Sprintf("%X", toEvict.tx.Hash()),
		//		"old_priority", toEvict.priority,
		//		"new_tx", fmt.Sprintf("%X", wtx.tx.Hash()),
		//		"new_priority", wtx.priority,
		//	)
		//	txmp.metrics.EvictedTxs.Add(1)
		//}
	}

	wtx.gasWanted = res.GasWanted
	wtx.priority = priority
	wtx.sender = sender
	wtx.peers = map[uint16]struct{}{
		txInfo.SenderID: {},
	}

	queueIdx := txmp.DistQueueIdx(wtx.evmAddress)
	err = txmp.TxQueues[queueIdx].AddTx(txmp, wtx)
	if err != nil {
		return err
	}
	txmp.logger.Debug(
		"inserted good transaction",
		"priority", wtx.priority,
		"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
		"height", txmp.height,
	)
	txmp.notifyTxsAvailable()

	return nil
}

func (txmp *FastTxMempool) notifyTxsAvailable() {

	if txmp.txsAvailable != nil && !txmp.notifiedTxsAvailable {
		// channel cap is 1, so this will send once
		txmp.notifiedTxsAvailable = true

		select {
		case txmp.txsAvailable <- struct{}{}:
		default:
		}
	}
}

// TxsAvailable returns a channel which fires once for every height, and only
// when transactions are available in the mempool. It is thread-safe.
func (txmp *FastTxMempool) TxsAvailable() <-chan struct{} {
	return txmp.txsAvailable
}

func (txmp *FastTxMempool) DistQueueIdx(sender string) int {
	length := len(sender)
	queuesNum := len(txmp.TxQueues)
	if length == 0 || queuesNum > 256 {
		return 0
	}

	cleanAddr := strings.TrimPrefix(sender, "0x")
	cleanAddr = strings.TrimPrefix(cleanAddr, "0X")

	return int((cleanAddr[0] + cleanAddr[5] + cleanAddr[15]) % uint8(queuesNum))
}

// canAddTx returns an error if we cannot insert the provided *WrappedTx into
// the mempool due to mempool configured constraints. If it returns nil,
// the transaction can be inserted into the mempool.
func (txmp *FastTxMempool) canAddTx(wtx *WrappedTx) error {
	var (
		numTxs    = txmp.Size()
		sizeBytes = txmp.SizeBytes()
	)

	if numTxs >= txmp.config.Size || int64(wtx.Size())+sizeBytes > txmp.config.MaxTxsBytes {
		return ErrMempoolIsFull{
			NumTxs:      numTxs,
			MaxTxs:      txmp.config.Size,
			TxsBytes:    sizeBytes,
			MaxTxsBytes: txmp.config.MaxTxsBytes,
		}
	}

	return nil
}

// AppendCheckTxErr wraps error message into an ABCIMessageLogs json string
func (txmp *FastTxMempool) AppendCheckTxErr(existingLogs string, log string) string {
	var builder strings.Builder

	builder.WriteString(existingLogs)
	// If there are already logs, append the new log with a separator
	if builder.Len() > 0 {
		builder.WriteString("; ")
	}
	builder.WriteString(log)

	return builder.String()
}
