// internal/lexer/token.go
package lexer

// TokenType 表示词法单元类型
type TokenType string

const (
	// 特殊标记
	EOF   TokenType = "EOF"   // 文件结束标记
	ERROR TokenType = "ERROR" // 错误标记

	// 关键字
	SELECT  TokenType = "SELECT"
	FROM    TokenType = "FROM"
	WHERE   TokenType = "WHERE"
	CREATE  TokenType = "CREATE"
	TABLE   TokenType = "TABLE"
	INSERT  TokenType = "INSERT"
	INTO    TokenType = "INTO"
	VALUES  TokenType = "VALUES"
	UPDATE  TokenType = "UPDATE"
	SET     TokenType = "SET"
	DELETE  TokenType = "DELETE"
	DROP    TokenType = "DROP"
	PRIMARY TokenType = "PRIMARY"
	KEY     TokenType = "KEY"
	INT     TokenType = "INT"
	TEXT    TokenType = "TEXT"
	LIKE    TokenType = "LIKE"

	// 标识符和字面量
	IDENT   TokenType = "IDENT"  // 标识符（如列名、表名）
	INT_LIT TokenType = "INT"    // 整数字面量
	STRING  TokenType = "STRING" // 字符串字面量

	// 运算符
	EQ TokenType = "="
	GT TokenType = ">"
	LT TokenType = "<"

	// 标识符
	COMMA    TokenType = ","
	SEMI     TokenType = ";"
	LPAREN   TokenType = "("
	RPAREN   TokenType = ")"
	ASTERISK TokenType = "*"
)

// Token 词法单元
// Type：标记的类型（如 SELECT、IDENT 等）
// Literal：标记的实际值（如具体的列名、数字等）
type Token struct {
	Type    TokenType // 标记类型
	Literal string    // 标记的实际值
}
