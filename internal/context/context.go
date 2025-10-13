// internal/context/context.go
package context

// DBContext 定义数据库上下文接口
type DBContext interface {
	GetDBName() string
	SetDBName(dbName string)
}
