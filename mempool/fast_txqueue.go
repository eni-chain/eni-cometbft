package mempool

import (
	"errors"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/types"
	"sync"
	"sync/atomic"
)

type AddressTxQueue struct {
	isFetch    []*WrappedTx
	pendingTxs map[uint64]*WrappedTx // key is nonce
}

func NewAddressTxQueue() *AddressTxQueue {
	return &AddressTxQueue{
		isFetch:    make([]*WrappedTx, 0),
		pendingTxs: make(map[uint64]*WrappedTx),
	}
}

func (queue *AddressTxQueue) AddTx(tx *WrappedTx) error {
	nextNonce := uint64(0)
	if len(queue.isFetch) != 0 {
		nextNonce = queue.isFetch[0].evmNonce
	}

	if !tx.isExpectedNextNonce {
		if nextNonce == 0 {
			queue.isFetch = append(queue.isFetch, tx)
			return nil
		} else {
			return errors.New("Tx already has a pending transaction ")
		}
	}

	if nextNonce == 0 {
		_, ok := queue.pendingTxs[tx.evmNonce]
		if !ok {
			queue.pendingTxs[tx.evmNonce] = tx
			return nil
		} else {
			return errors.New("Tx already has a pending transaction ")
		}
	} else {
		expectedNext := nextNonce + 1 + uint64(len(queue.isFetch))
		if tx.evmNonce < expectedNext {
			return errors.New("Tx already has a pending transaction ")
		} else if tx.evmNonce == expectedNext {
			queue.isFetch = append(queue.isFetch, tx)
			for len(queue.pendingTxs) != 0 {
				expectedNext += 1
				nTx, ok := queue.pendingTxs[expectedNext]
				if ok {
					queue.isFetch = append(queue.isFetch, nTx)
					delete(queue.pendingTxs, expectedNext)
				} else {
					return nil
				}
			}
		} else {
			queue.pendingTxs[tx.evmNonce] = tx
			return nil
		}
	}
	return nil
}

func (queue AddressTxQueue) DelTx(txmp *FastTxMempool, wtx *WrappedTx, removeFromCache bool) {
	for _, nTx := range queue.isFetch {
		if nTx.evmNonce <= wtx.evmNonce {
			queue.isFetch = queue.isFetch[1 : len(queue.isFetch)-1]
			//txmp.metrics.RemovedTxs.Add(1)
			atomic.AddInt64(&txmp.sizeBytes, int64(-nTx.Size()))
			atomic.AddInt64(&txmp.totalTxCnt, -1)

			// Remove the transaction from the gossip index and cleanup the linked-list
			// element so it can be garbage collected.
			txmp.gossipIndex.Remove(wtx.gossipEl)
			wtx.gossipEl.DetachPrev()
			nTx.removeHandler(removeFromCache)
		} else {
			break
		}
	}
}

func (queue AddressTxQueue) GetTxs() []*WrappedTx {
	return queue.isFetch
}

func (queue AddressTxQueue) GetFirstTx(tx *WrappedTx) *WrappedTx {
	if len(queue.isFetch) == 0 {
		return nil
	}
	return queue.isFetch[0]
}

type FastTxQueue struct {
	mtx sync.RWMutex

	cosmosTx map[types.TxKey]*WrappedTx
	evmTx    map[string]*AddressTxQueue //key is evmAddress  sorted by nonce

}

func newFastTxQueue(cfg *config.MempoolConfig) *FastTxQueue {
	queue := &FastTxQueue{
		mtx:      sync.RWMutex{},
		cosmosTx: make(map[types.TxKey]*WrappedTx),
		evmTx:    make(map[string]*AddressTxQueue),
	}

	return queue
}

func (txq *FastTxQueue) delTx(txmp *FastTxMempool, wtx *WrappedTx, removeFromCache bool, shouldReenqueue bool, updatePriorityIndex bool) {
	if txmp.txStore.IsTxRemoved(wtx) {
		return
	}
	txq.mtx.Lock()
	if wtx.isEVM {
		queue, ok := txq.evmTx[wtx.evmAddress]
		if ok {
			queue.DelTx(txmp, wtx, removeFromCache)
		}
	} else {
		delete(txq.cosmosTx, wtx.hash)
		// Remove the transaction from the gossip index and cleanup the linked-list
		// element so it can be garbage collected.
		txmp.gossipIndex.Remove(wtx.gossipEl)
		wtx.gossipEl.DetachPrev()
	}

	txq.mtx.Unlock()
	txmp.txStore.RemoveTx(wtx)

}

func (txq *FastTxQueue) DelTx(txmp *FastTxMempool, wtx *WrappedTx, removeFromCache bool, shouldReenqueue bool, updatePriorityIndex bool) {
	txq.delTx(txmp, wtx, removeFromCache, shouldReenqueue, updatePriorityIndex)
}

func (txq *FastTxQueue) AddTx(txmp *FastTxMempool, wtx *WrappedTx) error {
	txq.mtx.Lock()

	if !wtx.isEVM {
		_, ok := txq.cosmosTx[wtx.hash]
		if !ok {
			txq.cosmosTx[wtx.hash] = wtx
			gossipEl := txmp.gossipIndex.PushBack(wtx)
			wtx.gossipEl = gossipEl
		}
	} else {
		queue, ok := txq.evmTx[wtx.evmAddress]
		if !ok {
			queue = NewAddressTxQueue()
			txq.evmTx[wtx.evmAddress] = queue
		}
		err := queue.AddTx(wtx)
		if err != nil {
			return err
		}
		gossipEl := txmp.gossipIndex.PushBack(wtx)
		wtx.gossipEl = gossipEl
	}
	txq.mtx.Unlock()

	txmp.txStore.SetTx(wtx)
	//txmp.metrics.InsertedTxs.Add(1)
	atomic.AddInt64(&txmp.sizeBytes, int64(wtx.Size()))
	atomic.AddInt64(&txmp.totalTxCnt, 1)
	return nil
}

func (txq *FastTxQueue) ForEachTx(handler func(wtx *WrappedTx) bool) {
	txq.mtx.RLock()
	defer txq.mtx.RUnlock()

	for _, wtx := range txq.cosmosTx {
		handler(wtx)
	}
	for _, queue := range txq.evmTx {
		for _, wtx := range queue.GetTxs() {
			handler(wtx)
		}
	}
}
