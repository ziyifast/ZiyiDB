// internal/storage/transaction.go

package storage

import (
	"fmt"
	"sync"
	"time"
)

// TransactionStatus 事务状态
type TransactionStatus int

const (
	TxnActive TransactionStatus = iota
	TxnCommitted
	TxnAborted
)

// Transaction 事务结构
type Transaction struct {
	ID         uint64
	Status     TransactionStatus
	StartTime  time.Time
	CommitTime time.Time
	ReadSet    map[string]map[int]struct{} // 记录读取的表和行
	WriteSet   map[string]map[int]struct{} // 记录写入的表和行
	Backend    *MemoryBackend
	mu         sync.RWMutex
}

// VersionedCell 带版本的单元格
type VersionedCell struct {
	Data      Cell
	TxnID     uint64
	Timestamp time.Time
	Committed bool
}

// TransactionManager 事务管理器
type TransactionManager struct {
	transactions map[uint64]*Transaction
	nextTxnID    uint64
	mu           sync.RWMutex
}

// NewTransactionManager 创建新的事务管理器
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		transactions: make(map[uint64]*Transaction),
		nextTxnID:    1,
	}
}

// BeginTransaction 开始一个新事务
func (tm *TransactionManager) BeginTransaction(backend *MemoryBackend) *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn := &Transaction{
		ID:        tm.nextTxnID,
		Status:    TxnActive,
		StartTime: time.Now(),
		ReadSet:   make(map[string]map[int]struct{}),
		WriteSet:  make(map[string]map[int]struct{}),
		Backend:   backend,
	}

	tm.transactions[tm.nextTxnID] = txn
	tm.nextTxnID++

	return txn
}

// Commit 提交事务
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status != TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	// 设置提交时间
	t.CommitTime = time.Now()
	t.Status = TxnCommitted

	// 在实际的存储引擎中提交更改
	t.Backend.commitTransaction(t)

	return nil
}

// Rollback 回滚事务
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status != TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	t.Status = TxnAborted

	// 在实际的存储引擎中回滚更改
	t.Backend.rollbackTransaction(t)

	return nil
}

// AddToReadSet 添加读取记录到读取集
func (t *Transaction) AddToReadSet(tableName string, rowID int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.ReadSet[tableName] == nil {
		t.ReadSet[tableName] = make(map[int]struct{})
	}
	t.ReadSet[tableName][rowID] = struct{}{}
}

// AddToWriteSet 添加写入记录到写入集
func (t *Transaction) AddToWriteSet(tableName string, rowID int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.WriteSet[tableName] == nil {
		t.WriteSet[tableName] = make(map[int]struct{})
	}
	t.WriteSet[tableName][rowID] = struct{}{}
}
