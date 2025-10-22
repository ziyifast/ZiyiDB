// internal/storage/disk.go
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"ziyi.db.com/internal/ast"
	"ziyi.db.com/internal/context"
)

type DiskBackend struct {
	dataPath string
	mu       sync.RWMutex
	txnMgr   *TransactionManager
	base     *BaseEngine // 添加基类引用
}

func NewDiskBackend(dataPath string) *DiskBackend {
	// 确保数据目录存在
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		panic(fmt.Sprintf("无法创建数据目录: %v", err))
	}

	return &DiskBackend{
		dataPath: dataPath,
		txnMgr:   NewTransactionManager(),
		base:     &BaseEngine{},
	}
}

func (d *DiskBackend) Insert(databaseName string, stmt *ast.InsertStatement, txn *Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 加载表
	table, err := d.loadTable(databaseName, stmt.TableName)
	if err != nil {
		return err
	}

	// 使用通用逻辑处理插入
	err = d.base.Insert(table, stmt, txn, d.getVisibleRow, d.base.convertToCell, d.base.evaluateExpression)
	if err != nil {
		return err
	}

	// 保存表
	return d.saveTable(databaseName, stmt.TableName, table)
}

func (d *DiskBackend) Select(databaseName string, stmt *ast.SelectStatement, txn *Transaction) (*Results, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 加载表
	table, err := d.loadTable(databaseName, stmt.TableName)
	if err != nil {
		return nil, err
	}

	// 使用通用逻辑处理查询
	return d.base.Select(table, stmt, txn, d.getVisibleRow, d.base.evaluateWhereCondition)
}

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

	// 事务查询
	for i := len(versionedRow) - 1; i >= 0; i-- {
		version := versionedRow[i]
		if version.TxnID == txn.ID || version.Committed {
			row := make([]Cell, len(versionedRow))
			for j, v := range versionedRow {
				row[j] = v.Data
			}
			return row
		}
	}

	return nil
}

// getDatabasePath 获取数据库路径
func (d *DiskBackend) getDatabasePath(dbName string) string {
	return filepath.Join(d.dataPath, dbName)
}

// getTablePath 获取表路径
func (d *DiskBackend) getTablePath(dbName, tableName string) string {
	return filepath.Join(d.getDatabasePath(dbName), tableName+".json")
}

// loadDatabase 加载数据库
func (d *DiskBackend) loadDatabase(dbName string) (*Database, error) {
	dbPath := d.getDatabasePath(dbName)

	// 检查数据库是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("数据库 '%s' 不存在", dbName)
	}

	// 读取数据库元数据
	db := &Database{
		Name:   dbName,
		Tables: make(map[string]*Table),
	}

	// 读取所有表
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		tableName := filepath.Base(entry.Name())
		tableName = tableName[:len(tableName)-5] // 移除 .json 后缀

		table, err := d.loadTable(dbName, tableName)
		if err != nil {
			return nil, err
		}

		db.Tables[tableName] = table
	}

	return db, nil
}

// loadTable 加载表
func (d *DiskBackend) loadTable(dbName, tableName string) (*Table, error) {
	tablePath := d.getTablePath(dbName, tableName)

	file, err := os.Open(tablePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var table Table
	if err := json.NewDecoder(file).Decode(&table); err != nil {
		return nil, err
	}

	// 初始化索引和行锁
	table.Indexes = make(map[string]*Index)
	table.RowLocks = make(map[int]*sync.RWMutex)

	// 重建索引
	d.rebuildIndexes(&table)

	return &table, nil
}

// saveTable 保存表
func (d *DiskBackend) saveTable(dbName, tableName string, table *Table) error {
	tablePath := d.getTablePath(dbName, tableName)

	// 确保目录存在
	dbPath := d.getDatabasePath(dbName)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return err
	}

	// 创建临时文件
	tempPath := tablePath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	// 写入数据
	if err := json.NewEncoder(file).Encode(table); err != nil {
		file.Close()
		os.Remove(tempPath)
		return err
	}

	// 关闭文件
	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}

	// 原子性替换
	return os.Rename(tempPath, tablePath)
}

// rebuildIndexes 重建索引
func (d *DiskBackend) rebuildIndexes(table *Table) {
	// 为主键创建索引
	for _, col := range table.Columns {
		if col.Primary {
			table.Indexes[col.Name] = &Index{
				Column: col.Name,
				Values: make(map[string][]int),
			}
		}
	}

	//// 重建索引数据
	//for i, row := range table.Rows {
	//	for _, col := range table.Columns {
	//		if col.Primary {
	//			key := row[col.Name].String()
	//			table.Indexes[col.Name].Values[key] = append(table.Indexes[col.Name].Values[key], i)
	//		}
	//	}
	//}
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
	return os.MkdirAll(dbPath, 0755)
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
	return os.RemoveAll(dbPath)
}

func (d *DiskBackend) UseDatabase(stmt *ast.UseDatabaseStatement, connCtx context.DBContext) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	dbPath := d.getDatabasePath(stmt.Name)

	// 检查数据库是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
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

	entries, err := os.ReadDir(d.dataPath)
	if err != nil {
		return results
	}

	for _, entry := range entries {
		if entry.IsDir() {
			results.Rows = append(results.Rows, []Cell{
				{Type: CellTypeText, TextValue: entry.Name()},
			})
		}
	}

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

	dbPath := d.getDatabasePath(dbName)
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return results
	}

	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			tableName := entry.Name()
			tableName = tableName[:len(tableName)-5] // 移除 .json 后缀
			results.Rows = append(results.Rows, []Cell{
				{Type: CellTypeText, TextValue: tableName},
			})
		}
	}

	return results
}

func (d *DiskBackend) CreateTable(databaseName string, stmt *ast.CreateTableStatement) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 加载数据库
	db, err := d.loadDatabase(databaseName)
	if err != nil {
		return err
	}

	// 检查表是否已存在
	if _, exists := db.Tables[stmt.TableName]; exists {
		return fmt.Errorf("表 '%s' 已存在", stmt.TableName)
	}

	// 创建新表
	table := &Table{
		Name:    stmt.TableName,
		Columns: stmt.Columns,
		Rows:    make([][]VersionedCell, 0),
	}

	// 保存表到磁盘
	if err := d.saveTable(databaseName, stmt.TableName, table); err != nil {
		return err
	}

	return nil
}

func (d *DiskBackend) DropTable(databaseName string, stmt *ast.DropTableStatement) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 检查数据库是否存在
	dbPath := d.getDatabasePath(databaseName)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", databaseName)
	}

	// 删除表文件
	tablePath := d.getTablePath(databaseName, stmt.TableName)
	if err := os.Remove(tablePath); err != nil {
		return fmt.Errorf("can not delete table '%s': %v", stmt.TableName, err)
	}

	return nil
}

func (d *DiskBackend) Update(databaseName string, stmt *ast.UpdateStatement, txn *Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 加载表
	table, err := d.loadTable(databaseName, stmt.TableName)
	if err != nil {
		return err
	}

	// 使用通用逻辑处理更新
	err = d.base.Update(table, stmt, txn, d.getVisibleRow, d.base.convertToCell,
		d.base.evaluateExpression, d.base.evaluateWhereCondition)
	if err != nil {
		return err
	}

	// 保存表
	return d.saveTable(databaseName, stmt.TableName, table)
}

func (d *DiskBackend) Delete(databaseName string, stmt *ast.DeleteStatement, txn *Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 加载表
	table, err := d.loadTable(databaseName, stmt.TableName)
	if err != nil {
		return err
	}

	// 使用通用逻辑处理删除
	err = d.base.Delete(table, stmt, txn, d.getVisibleRow, d.base.evaluateWhereCondition)
	if err != nil {
		return err
	}

	// 保存表
	return d.saveTable(databaseName, stmt.TableName, table)
}
func (d *DiskBackend) BeginTransaction() *Transaction {
	return d.txnMgr.BeginTransaction(nil) // 简化处理
}
