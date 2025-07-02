package mempool

import (
	"github.com/cometbft/cometbft/libs/clist"
	"testing"
)

func TestAddressTxQueue_DelTx(t *testing.T) {

	// 初始化 FastTxMempool 和 AddressTxQueue
	txmp := &FastTxMempool{
		gossipIndex: clist.New(), // 使用 clist 包初始化链表
		sizeBytes:   0,
		totalTxCnt:  0,
	}

	queue := &AddressTxQueue{
		isFetch:    make([]*WrappedTx, 0),
		pendingTxs: make(map[uint64]*WrappedTx),
	}

	wtx1 := &WrappedTx{
		evmNonce:      1,
		removeHandler: func(removeFromCache bool) {},
	}

	wtx2 := &WrappedTx{
		evmNonce:      2,
		removeHandler: func(removeFromCache bool) {},
	}

	wtx1.gossipEl = txmp.gossipIndex.PushBack(wtx1)
	wtx2.gossipEl = txmp.gossipIndex.PushBack(wtx2)
	queue.isFetch = append(queue.isFetch, wtx1, wtx2)

	t.Run("testDelTx", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				errMsg, ok := r.(string)
				if !ok {
					t.Errorf("panic: %v", errMsg)
				}
			} else {
				t.Logf("Success")
			}
		}()

		queue.DelTx(txmp, wtx2, false)
	})
}
