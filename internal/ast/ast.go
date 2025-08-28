// internal/ast/ast.go
package ast

import (
	"time"
	"ziyi.db.com/internal/lexer"
)

// Node 表示AST中的节点,所有 AST 节点的基本接口
type Node interface {
	TokenLiteral() string
}

// Statement 表示SQL语句的接口
type Statement interface {
	Node
	statementNode()
}

// Expression 表示表达式的接口
type Expression interface {
	Node
	expressionNode()
}

type BaseExpression struct {
	Token lexer.Token
}

func (b *BaseExpression) expressionNode()      {}
func (b *BaseExpression) TokenLiteral() string { return b.Token.Literal }

// Program 表示整个SQL程序
// 包含多个 SQL 语句
type Program struct {
	Statements []Statement
}

// SelectStatement 表示SELECT语句
// 表示 SELECT 查询语句
// 包含选择的字段、表名和 WHERE 条件
type SelectStatement struct {
	Token     lexer.Token
	Fields    []Expression
	TableName string
	Where     Expression
}

func (ss *SelectStatement) statementNode()       {}
func (ss *SelectStatement) TokenLiteral() string { return ss.Token.Literal }

// CreateTableStatement 表示CREATE TABLE语句
// 表示创建表的语句
// 包含表名和列定义
type CreateTableStatement struct {
	Token     lexer.Token
	TableName string
	Columns   []ColumnDefinition
}

func (cts *CreateTableStatement) statementNode()       {}
func (cts *CreateTableStatement) TokenLiteral() string { return cts.Token.Literal }

// InsertStatement 表示INSERT语句
// 表示插入数据的语句
// 包含表名和要插入的值
type InsertStatement struct {
	Token     lexer.Token
	TableName string
	Columns   []*Identifier // 列名列表
	Values    []Expression
}

func (is *InsertStatement) statementNode()       {}
func (is *InsertStatement) TokenLiteral() string { return is.Token.Literal }

// ColumnDefinition 表示列定义
// 列定义包含名称、类型、主键和可空性
type ColumnDefinition struct {
	Name     string
	Type     string
	Primary  bool
	Nullable bool
	Default  interface{} //列默认值
}

// StarExpression 表示星号表达式
type StarExpression struct{}

func (se *StarExpression) expressionNode()      {}
func (se *StarExpression) TokenLiteral() string { return "*" }

// LikeExpression 表示LIKE表达式
type LikeExpression struct {
	Token   lexer.Token
	Left    Expression
	Pattern string
}

func (le *LikeExpression) expressionNode()      {}
func (le *LikeExpression) TokenLiteral() string { return le.Token.Literal }

// BinaryExpression 表示二元表达式
type BinaryExpression struct {
	Token    lexer.Token
	Left     Expression
	Operator string
	Right    Expression
}

func (be *BinaryExpression) expressionNode()      {}
func (be *BinaryExpression) TokenLiteral() string { return be.Token.Literal }

// IntegerLiteral 表示整数字面量
type IntegerLiteral struct {
	BaseExpression
	Token lexer.Token
	Value int32 // 修改为实际的int32值
}

// FloatLiteral 表示浮点数字面量
type FloatLiteral struct {
	BaseExpression
	Token lexer.Token
	Value float32 // 修改为实际的float32值
}

// DateTimeLiteral 表示日期时间字面量
type DateTimeLiteral struct {
	BaseExpression
	Token lexer.Token
	Value time.Time // 修改为实际的time.Time值
}

// StringLiteral 表示字符串字面量
type StringLiteral struct {
	BaseExpression
	Token lexer.Token
	Value string
}

// BetweenExpression 表示 BETWEEN AND 表达式（新增）
type BetweenExpression struct {
	Token lexer.Token // BETWEEN 标记
	Left  Expression  // 左操作数（列名或表达式）
	Low   Expression  // 下限值
	High  Expression  // 上限值
}

func (be *BetweenExpression) expressionNode()      {}
func (be *BetweenExpression) TokenLiteral() string { return be.Token.Literal }

// Identifier 表示标识符
type Identifier struct {
	Token lexer.Token
	Value string
}

func (i *Identifier) expressionNode()      {}
func (i *Identifier) TokenLiteral() string { return i.Token.Literal }

// UpdateStatement 表示UPDATE语句
// 表示更新数据的语句
// 包含表名、SET 子句和 WHERE 条件
type UpdateStatement struct {
	Token     lexer.Token
	TableName string
	Set       []SetClause
	Where     Expression
}

func (us *UpdateStatement) statementNode()       {}
func (us *UpdateStatement) TokenLiteral() string { return us.Token.Literal }

// SetClause 表示SET子句
type SetClause struct {
	Column string
	Value  Expression
}

// DeleteStatement 表示DELETE语句
// 表示删除数据的语句
// 包含表名和 WHERE 条件
type DeleteStatement struct {
	Token     lexer.Token
	TableName string
	Where     Expression
}

func (ds *DeleteStatement) statementNode()       {}
func (ds *DeleteStatement) TokenLiteral() string { return ds.Token.Literal }

// DropTableStatement 表示DROP TABLE语句
// 表示删除表的语句
// 包含表名
type DropTableStatement struct {
	Token     lexer.Token
	TableName string
}

func (ds *DropTableStatement) statementNode()       {}
func (ds *DropTableStatement) TokenLiteral() string { return ds.Token.Literal }

// DefaultExpression 表示DEFAULT表达式
type DefaultExpression struct {
	Token lexer.Token
	Value Expression
}

func (de *DefaultExpression) expressionNode()      {}
func (de *DefaultExpression) TokenLiteral() string { return de.Token.Literal }

// FunctionCall 表示函数调用
type FunctionCall struct {
	Token  lexer.Token
	Name   string
	Params []Expression
}

func (fc *FunctionCall) expressionNode()      {}
func (fc *FunctionCall) TokenLiteral() string { return fc.Token.Literal }
