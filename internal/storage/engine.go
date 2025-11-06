// internal/storage/engine.go
package storage

import (
	"ziyi.db.com/internal/ast"
	"ziyi.db.com/internal/context"
)

// Engine 定义存储引擎接口
type Engine interface {
	// 数据库操作
	CreateDatabase(stmt *ast.CreateDatabaseStatement) error
	DropDatabase(stmt *ast.DropDatabaseStatement) error
	UseDatabase(stmt *ast.UseDatabaseStatement, connCtx context.DBContext) error
	ShowDatabases() *Results
	ShowTables(connCtx context.DBContext) *Results

	// 表操作
	CreateTable(databaseName string, stmt *ast.CreateTableStatement) error
	DropTable(databaseName string, stmt *ast.DropTableStatement) error

	// 数据操作
	Insert(databaseName string, stmt *ast.InsertStatement, txn *Transaction) error
	Select(databaseName string, stmt *ast.SelectStatement, txn *Transaction) (*Results, error)
	Update(databaseName string, stmt *ast.UpdateStatement, txn *Transaction) error
	Delete(databaseName string, stmt *ast.DeleteStatement, txn *Transaction) error

	// 事务支持
	BeginTransaction() *Transaction

	// 提交事务
	CommitTransaction(txn *Transaction) error

	// 回滚事务
	RollbackTransaction(txn *Transaction) error
}
