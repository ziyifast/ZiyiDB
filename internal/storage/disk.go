// internal/storage/disk.go
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	"ziyi.db.com/internal/ast"
	"ziyi.db.com/internal/context"
)

type DiskBackend struct {
	dataPath string
	mu       sync.RWMutex
	txnMgr   *TransactionManager
	base     *BaseEngine
	// 数据字典，存储所有表的元数据。参考MySQL8.0设计
	dataDictionary map[string]map[string]*TableMetadata
	dictMutex      sync.RWMutex

	// 事务表缓存，存储事务期间的表修改
	// key: txnID, value: map[tableName]*Table
	txnTableCache map[uint64]map[string]*Table
	cacheMutex    sync.RWMutex
}

// TableMetadata 表示表的元数据（替代.frm文件）
type TableMetadata struct {
	Name    string
	Columns []ast.ColumnDefinition
	Indexes map[string]*IndexMetadata
}

// IndexMetadata 索引元数据
type IndexMetadata struct {
	Column string
	Type   string // "PRIMARY", "UNIQUE", "INDEX"
}

func NewDiskBackend(dataPath string) *DiskBackend {
	// 确保数据目录存在
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		panic(fmt.Sprintf("无法创建数据目录: %v", err))
	}

	backend := &DiskBackend{
		dataPath:       dataPath,
		txnMgr:         NewTransactionManager(),
		base:           &BaseEngine{},
		dataDictionary: make(map[string]map[string]*TableMetadata),
		txnTableCache:  make(map[uint64]map[string]*Table),
	}

	// 初始化数据字典
	backend.initDataDictionary()

	return backend
}

// initDataDictionary 初始化数据字典
func (d *DiskBackend) initDataDictionary() {
	dictPath := filepath.Join(d.dataPath, "data_dictionary.json")

	// 尝试加载现有的数据字典
	if file, err := os.Open(dictPath); err == nil {
		defer file.Close()
		var dict map[string]map[string]*TableMetadata
		if err := json.NewDecoder(file).Decode(&dict); err == nil {
			d.dataDictionary = dict
			return
		}
	}

	// 如果没有现有字典，创建新的
	d.dataDictionary = make(map[string]map[string]*TableMetadata)
}

// saveDataDictionary 保存数据字典到磁盘
func (d *DiskBackend) saveDataDictionary() error {
	dictPath := filepath.Join(d.dataPath, "data_dictionary.json")
	tempPath := dictPath + ".tmp"

	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	if err := json.NewEncoder(file).Encode(d.dataDictionary); err != nil {
		file.Close()
		os.Remove(tempPath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}

	return os.Rename(tempPath, dictPath)
}

// getDatabasePath 获取数据库路径
func (d *DiskBackend) getDatabasePath(dbName string) string {
	return filepath.Join(d.dataPath, dbName)
}

// getTableDataPath 获取表数据文件路径 (.ibd)
func (d *DiskBackend) getTableDataPath(dbName, tableName string) string {
	return filepath.Join(d.getDatabasePath(dbName), tableName+".ibd")
}

// loadTable 加载表（从.ibd文件和数据字典）
func (d *DiskBackend) loadTable(dbName, tableName string, txn *Transaction) (*Table, error) {
	// 如果在事务中，首先检查事务缓存
	if txn != nil {
		d.cacheMutex.RLock()
		if tables, exists := d.txnTableCache[txn.ID]; exists {
			if table, exists := tables[tableName]; exists {
				d.cacheMutex.RUnlock()
				// 直接返回缓存中的表引用，而不是创建副本
				// 这样可以确保在事务中看到最新的修改
				return table, nil
			}
		}
		d.cacheMutex.RUnlock()
	}

	d.dictMutex.RLock()
	// 从数据字典获取表结构
	dbTables, dbExists := d.dataDictionary[dbName]
	if !dbExists {
		d.dictMutex.RUnlock()
		return nil, fmt.Errorf("database '%s' not exist", dbName)
	}

	metadata, tableExists := dbTables[tableName]
	if !tableExists {
		d.dictMutex.RUnlock()
		return nil, fmt.Errorf("table '%s' not exist", tableName)
	}
	d.dictMutex.RUnlock()

	// 创建表结构
	table := &Table{
		Name:     metadata.Name,
		Columns:  metadata.Columns,
		Indexes:  make(map[string]*Index),
		RowLocks: make(map[int]*sync.RWMutex),
		Rows:     make([][]VersionedCell, 0),
	}

	// 恢复索引结构
	for name, idxMeta := range metadata.Indexes {
		table.Indexes[name] = &Index{
			Column: idxMeta.Column,
			Values: make(map[string][]int),
		}
	}

	// 加载表数据
	dataPath := d.getTableDataPath(dbName, tableName)
	if dataFile, err := os.Open(dataPath); err == nil {
		defer dataFile.Close()

		var tableData struct {
			Rows [][]VersionedCell `json:"rows"`
		}
		if err := json.NewDecoder(dataFile).Decode(&tableData); err == nil {
			table.Rows = tableData.Rows
		}
	}

	// 重建索引数据
	d.rebuildIndexes(table)

	// 如果在事务中，将加载的表放入缓存
	if txn != nil {
		d.cacheMutex.Lock()
		if d.txnTableCache[txn.ID] == nil {
			d.txnTableCache[txn.ID] = make(map[string]*Table)
		}
		// 存储表的引用到缓存中
		d.txnTableCache[txn.ID][tableName] = table
		d.cacheMutex.Unlock()
	}

	return table, nil
}

// saveTable 保存表（保存到.ibd文件，元数据保存在数据字典中）
func (d *DiskBackend) saveTable(dbName, tableName string, table *Table) error {
	dbPath := d.getDatabasePath(dbName)
	// 确保数据库目录存在
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return err
	}
	// 更新数据字典中的表元数据
	d.dictMutex.Lock()
	if d.dataDictionary[dbName] == nil {
		d.dataDictionary[dbName] = make(map[string]*TableMetadata)
	}
	// 创建表元数据
	metadata := &TableMetadata{
		Name:    table.Name,
		Columns: table.Columns,
		Indexes: make(map[string]*IndexMetadata),
	}
	// 保存索引元数据
	for name, idx := range table.Indexes {
		indexType := "INDEX"
		for _, col := range table.Columns {
			if col.Name == idx.Column && col.Primary {
				indexType = "PRIMARY"
				break
			}
		}
		metadata.Indexes[name] = &IndexMetadata{
			Column: idx.Column,
			Type:   indexType,
		}
	}
	d.dataDictionary[dbName][tableName] = metadata
	// 保存数据字典
	if err := d.saveDataDictionary(); err != nil {
		d.dictMutex.Unlock()
		return err
	}
	d.dictMutex.Unlock()
	// 保存表数据到.ibd文件
	dataPath := d.getTableDataPath(dbName, tableName)
	tempPath := dataPath + ".tmp"
	dataFile, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	// 只保存数据行
	tableData := struct {
		Rows [][]VersionedCell `json:"rows"`
	}{
		Rows: table.Rows,
	}
	if err := json.NewEncoder(dataFile).Encode(tableData); err != nil {
		dataFile.Close()
		os.Remove(tempPath)
		return err
	}
	if err := dataFile.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}
	// 原子性替换表数据文件
	return os.Rename(tempPath, dataPath)
}

// rebuildIndexes 重建索引
func (d *DiskBackend) rebuildIndexes(table *Table) {
	// 清空现有索引值
	for _, index := range table.Indexes {
		index.Values = make(map[string][]int)
	}

	// 重建索引数据
	for i, row := range table.Rows {
		for _, col := range table.Columns {
			if col.Primary {
				if i < len(row) {
					key := row[i].Data.String()
					if index, exists := table.Indexes[col.Name]; exists {
						index.Values[key] = append(index.Values[key], i)
					}
				}
			}
		}
	}
}

// 实现 Engine 接口的方法
func (d *DiskBackend) CreateDatabase(stmt *ast.CreateDatabaseStatement) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	dbPath := d.getDatabasePath(stmt.Name)

	// 检查数据库是否已存在
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		return fmt.Errorf("数据库 '%s' 已存在", stmt.Name)
	}

	// 创建数据库目录
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return err
	}

	// 更新数据字典
	d.dictMutex.Lock()
	d.dataDictionary[stmt.Name] = make(map[string]*TableMetadata)
	err := d.saveDataDictionary()
	d.dictMutex.Unlock()

	return err
}

func (d *DiskBackend) DropDatabase(stmt *ast.DropDatabaseStatement) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	dbPath := d.getDatabasePath(stmt.Name)

	// 检查数据库是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", stmt.Name)
	}

	// 删除数据库目录
	if err := os.RemoveAll(dbPath); err != nil {
		return err
	}

	// 更新数据字典
	d.dictMutex.Lock()
	delete(d.dataDictionary, stmt.Name)
	err := d.saveDataDictionary()
	d.dictMutex.Unlock()

	return err
}

func (d *DiskBackend) UseDatabase(stmt *ast.UseDatabaseStatement, connCtx context.DBContext) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 检查数据库是否存在（通过数据字典）
	d.dictMutex.RLock()
	_, exists := d.dataDictionary[stmt.Name]
	d.dictMutex.RUnlock()

	if !exists {
		return fmt.Errorf("database '%s' does not exist", stmt.Name)
	}

	// 更新连接上下文中的当前数据库
	connCtx.SetDBName(stmt.Name)
	return nil
}

func (d *DiskBackend) ShowDatabases() *Results {
	d.mu.RLock()
	defer d.mu.RUnlock()

	results := &Results{
		Columns: []ResultColumn{
			{Name: "Database", Type: "TEXT"},
		},
		Rows: make([][]Cell, 0),
	}

	d.dictMutex.RLock()
	for dbName := range d.dataDictionary {
		results.Rows = append(results.Rows, []Cell{
			{Type: CellTypeText, TextValue: dbName},
		})
	}
	d.dictMutex.RUnlock()

	return results
}

func (d *DiskBackend) ShowTables(connCtx context.DBContext) *Results {
	d.mu.RLock()
	defer d.mu.RUnlock()

	results := &Results{
		Columns: []ResultColumn{
			{Name: "Tables", Type: "TEXT"},
		},
		Rows: make([][]Cell, 0),
	}

	dbName := connCtx.GetDBName()
	if dbName == "" {
		return results
	}

	d.dictMutex.RLock()
	if dbTables, exists := d.dataDictionary[dbName]; exists {
		for tableName := range dbTables {
			results.Rows = append(results.Rows, []Cell{
				{Type: CellTypeText, TextValue: tableName},
			})
		}
	}
	d.dictMutex.RUnlock()

	return results
}

func (d *DiskBackend) CreateTable(databaseName string, stmt *ast.CreateTableStatement) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 检查数据库是否存在
	d.dictMutex.RLock()
	_, dbExists := d.dataDictionary[databaseName]
	d.dictMutex.RUnlock()

	if !dbExists {
		return fmt.Errorf("database '%s' does not exist", databaseName)
	}

	// 检查表是否已存在
	d.dictMutex.RLock()
	_, tableExists := d.dataDictionary[databaseName][stmt.TableName]
	d.dictMutex.RUnlock()

	if tableExists {
		return fmt.Errorf("表 '%s' 已存在", stmt.TableName)
	}

	// 创建新表
	table := &Table{
		Name:     stmt.TableName,
		Columns:  stmt.Columns,
		Rows:     make([][]VersionedCell, 0),
		Indexes:  make(map[string]*Index),
		RowLocks: make(map[int]*sync.RWMutex),
	}

	// 为主键创建索引
	for _, col := range stmt.Columns {
		if col.Primary {
			table.Indexes[col.Name] = &Index{
				Column: col.Name,
				Values: make(map[string][]int),
			}
		}
	}

	// 保存表到磁盘
	return d.saveTable(databaseName, stmt.TableName, table)
}

func (d *DiskBackend) DropTable(databaseName string, stmt *ast.DropTableStatement) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 检查数据库是否存在
	d.dictMutex.RLock()
	_, dbExists := d.dataDictionary[databaseName]
	d.dictMutex.RUnlock()

	if !dbExists {
		return fmt.Errorf("database '%s' does not exist", databaseName)
	}

	// 检查表是否存在
	d.dictMutex.RLock()
	_, tableExists := d.dataDictionary[databaseName][stmt.TableName]
	d.dictMutex.RUnlock()

	if !tableExists {
		return fmt.Errorf("table '%s' does not exist", stmt.TableName)
	}

	// 删除表数据文件
	dataPath := d.getTableDataPath(databaseName, stmt.TableName)
	if err := os.Remove(dataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("can not delete table data '%s': %v", stmt.TableName, err)
	}

	// 从数据字典中移除表
	d.dictMutex.Lock()
	delete(d.dataDictionary[databaseName], stmt.TableName)
	err := d.saveDataDictionary()
	d.dictMutex.Unlock()

	return err
}

// Select 查询数据
func (d *DiskBackend) Select(databaseName string, stmt *ast.SelectStatement, txn *Transaction) (*Results, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 加载表（支持事务缓存）
	table, err := d.loadTable(databaseName, stmt.TableName, txn)
	if err != nil {
		return nil, err
	}

	// 使用通用逻辑处理查询
	return d.base.Select(table, stmt, txn, d.getVisibleRow, d.base.evaluateWhereCondition)
}

// Insert 插入数据
func (d *DiskBackend) Insert(databaseName string, stmt *ast.InsertStatement, txn *Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 加载表（支持事务缓存）
	table, err := d.loadTable(databaseName, stmt.TableName, txn)
	if err != nil {
		return err
	}

	// 使用通用逻辑处理插入
	err = d.base.Insert(table, stmt, txn, d.getVisibleRow, d.base.convertToCell, d.base.evaluateExpression)
	if err != nil {
		return err
	}

	// 在非事务模式下直接保存表，否则更新事务缓存
	if txn == nil {
		return d.saveTable(databaseName, stmt.TableName, table)
	} else {
		// 更新事务缓存中的表
		d.cacheMutex.Lock()
		if d.txnTableCache[txn.ID] == nil {
			d.txnTableCache[txn.ID] = make(map[string]*Table)
		}
		d.txnTableCache[txn.ID][stmt.TableName] = table
		d.cacheMutex.Unlock()
	}

	return nil
}

// Update 更新数据
func (d *DiskBackend) Update(databaseName string, stmt *ast.UpdateStatement, txn *Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 加载表（支持事务缓存）
	table, err := d.loadTable(databaseName, stmt.TableName, txn)
	if err != nil {
		return err
	}

	// 使用通用逻辑处理更新
	err = d.base.Update(table, stmt, txn, d.getVisibleRow, d.base.convertToCell,
		d.base.evaluateExpression, d.base.evaluateWhereCondition)
	if err != nil {
		return err
	}

	// 在非事务模式下直接保存表，否则更新事务缓存
	if txn == nil {
		return d.saveTable(databaseName, stmt.TableName, table)
	} else {
		// 更新事务缓存中的表
		d.cacheMutex.Lock()
		if d.txnTableCache[txn.ID] == nil {
			d.txnTableCache[txn.ID] = make(map[string]*Table)
		}
		d.txnTableCache[txn.ID][stmt.TableName] = table
		d.cacheMutex.Unlock()
	}

	return nil
}

// Delete 删除数据
func (d *DiskBackend) Delete(databaseName string, stmt *ast.DeleteStatement, txn *Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 加载表（支持事务缓存）
	table, err := d.loadTable(databaseName, stmt.TableName, txn)
	if err != nil {
		return err
	}

	// 使用通用逻辑处理删除
	err = d.base.Delete(table, stmt, txn, d.getVisibleRow, d.base.evaluateWhereCondition)
	if err != nil {
		return err
	}

	// 在非事务模式下直接保存表，否则更新事务缓存
	if txn == nil {
		return d.saveTable(databaseName, stmt.TableName, table)
	} else {
		// 更新事务缓存中的表
		d.cacheMutex.Lock()
		if d.txnTableCache[txn.ID] == nil {
			d.txnTableCache[txn.ID] = make(map[string]*Table)
		}
		d.txnTableCache[txn.ID][stmt.TableName] = table
		d.cacheMutex.Unlock()
	}

	return nil
}

// updateIndexesAfterInsert 在插入后更新索引
func (d *DiskBackend) updateIndexesAfterInsert(table *Table, oldRowCount int) {
	// 只需要更新新插入行的索引
	for i := oldRowCount; i < len(table.Rows); i++ {
		row := table.Rows[i]
		for _, col := range table.Columns {
			if col.Primary && i < len(row) {
				key := row[i].Data.String()
				if index, exists := table.Indexes[col.Name]; exists {
					index.Values[key] = append(index.Values[key], i)
				}
			}
		}
	}
}

func (d *DiskBackend) BeginTransaction() *Transaction {
	return d.txnMgr.BeginTransaction(d)
}

// CommitTransaction 提交事务中的更改
func (d *DiskBackend) CommitTransaction(txn *Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 首先调用事务管理器的提交方法
	if err := d.txnMgr.CommitTransaction(txn); err != nil {
		return err
	}

	// 获取事务缓存中的表
	d.cacheMutex.RLock()
	tables, cacheExists := d.txnTableCache[txn.ID]
	d.cacheMutex.RUnlock()

	if cacheExists {
		// 遍历事务缓存中的所有表，将修改持久化到磁盘
		for tableName, table := range tables {
			// 查找表所在的数据库
			var dbName string
			var found bool

			d.dictMutex.RLock()
			for db, dbTables := range d.dataDictionary {
				if _, exists := dbTables[tableName]; exists {
					dbName = db
					found = true
					break
				}
			}
			d.dictMutex.RUnlock()

			if !found {
				continue
			}

			// 更新表中所有版本化单元格的状态
			for i := range table.Rows {
				for j := range table.Rows[i] {
					if table.Rows[i][j].TxnID == txn.ID {
						table.Rows[i][j].Committed = true
						table.Rows[i][j].Timestamp = time.Now()
					}
				}
			}

			// 保存表到磁盘
			if err := d.saveTable(dbName, tableName, table); err != nil {
				return err
			}
		}
	}

	// 清理事务缓存
	d.cacheMutex.Lock()
	delete(d.txnTableCache, txn.ID)
	d.cacheMutex.Unlock()

	// 保存数据字典
	return d.saveDataDictionary()
}

// RollbackTransaction 回滚事务中的更改
func (d *DiskBackend) RollbackTransaction(txn *Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 首先调用事务管理器的回滚方法
	if err := d.txnMgr.RollbackTransaction(txn); err != nil {
		return err
	}

	// 清理事务缓存（回滚时直接丢弃所有未提交的修改）
	d.cacheMutex.Lock()
	delete(d.txnTableCache, txn.ID)
	d.cacheMutex.Unlock()

	// 保存数据字典
	return d.saveDataDictionary()
}

// getVisibleRow 获取可见行版本
func (d *DiskBackend) getVisibleRow(versionedRow []VersionedCell, txn *Transaction) []Cell {
	if len(versionedRow) == 0 {
		return nil
	}

	if txn == nil {
		// 非事务查询，返回最新提交的版本
		for i := len(versionedRow) - 1; i >= 0; i-- {
			if versionedRow[i].Committed {
				row := make([]Cell, len(versionedRow))
				for j, v := range versionedRow {
					row[j] = v.Data
				}
				return row
			}
		}
		return nil
	}

	// 事务查询，根据读已提交隔离级别规则
	// 查找对当前事务可见的最新版本
	for i := len(versionedRow) - 1; i >= 0; i-- {
		version := versionedRow[i]

		// 如果是当前事务自己的修改，可见（即使未提交）
		if version.TxnID == txn.ID {
			row := make([]Cell, len(versionedRow))
			for j, v := range versionedRow {
				row[j] = v.Data
			}
			return row
		}

		// 对于读已提交隔离级别，只能看到已提交的数据
		if version.Committed {
			row := make([]Cell, len(versionedRow))
			for j, v := range versionedRow {
				row[j] = v.Data
			}
			return row
		}
	}

	return nil
}
