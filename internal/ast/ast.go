// internal/ast/ast.go
package ast

import (
	"fmt"
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
}

// Cell 表示单元格
type Cell struct {
	Type       CellType
	IntValue   int32
	TextValue  string
	FloatValue float32 // 用于Float
	TimeValue  string
}

// CellType 表示单元格类型
type CellType int

const (
	CellTypeInt CellType = iota
	CellTypeText
	CellTypeFloat
	CellTypeDateTime
)

// AsText 返回单元格的文本值
func (c *Cell) AsText() string {
	switch c.Type {
	case CellTypeInt:
		s := fmt.Sprintf("%d", c.IntValue)
		return s
	case CellTypeText:
		return c.TextValue
	case CellTypeFloat:
		return fmt.Sprintf("%.4f", c.FloatValue)
	case CellTypeDateTime:
		return c.TextValue // 时间格式为存储为字符串格式（如"2023-10-01 12:34:56"）
	default:
		return "NULL"
	}
}

// AsInt 返回单元格的整数值
func (c *Cell) AsInt() int32 {
	if c.Type == CellTypeInt {
		return c.IntValue
	}
	return 0
}

// AsFloat 返回单元格的浮点数值
func (c *Cell) AsFloat() float32 {
	if c.Type == CellTypeFloat {
		return c.FloatValue
	}
	return 0.0
}

// String 返回单元格的字符串表示
func (c Cell) String() string {
	switch c.Type {
	case CellTypeInt:
		return fmt.Sprintf("%d", c.IntValue)
	case CellTypeText:
		return c.TextValue
	case CellTypeFloat:
		return fmt.Sprintf("%.2f", c.FloatValue)
	case CellTypeDateTime:
		return c.TimeValue
	default:
		return "NULL"
	}
}

// Results 表示查询结果
type Results struct {
	Columns []ResultColumn
	Rows    [][]Cell
}

// ResultColumn 表示结果列
type ResultColumn struct {
	Name string
	Type string
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
	Token lexer.Token
	Value string
}

func (il *IntegerLiteral) expressionNode()      {}
func (il *IntegerLiteral) TokenLiteral() string { return il.Token.Literal }

// StringLiteral 表示字符串字面量
type StringLiteral struct {
	Token lexer.Token
	Value string
}

func (sl *StringLiteral) expressionNode()      {}
func (sl *StringLiteral) TokenLiteral() string { return sl.Token.Literal }

// 新增FloatLiteral表达式类型
type FloatLiteral struct {
	Token lexer.Token
	Value string // 存储原始字符串（如"123.45"）或转换为float64
}

func (fl *FloatLiteral) expressionNode()      {}
func (fl *FloatLiteral) TokenLiteral() string { return fl.Token.Literal }

// 新增DateTimeLiteral表达式类型
type DateTimeLiteral struct {
	Token lexer.Token
	Value string
}

func (tl *DateTimeLiteral) expressionNode()      {}
func (tl *DateTimeLiteral) TokenLiteral() string { return tl.Token.Literal }

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
