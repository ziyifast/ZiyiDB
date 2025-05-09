// internal/lexer/lexer.go
package lexer

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"unicode"
)

// Lexer 词法分析器
// reader：使用 bufio.Reader 进行高效的字符读取
// ch：存储当前正在处理的字符
type Lexer struct {
	reader *bufio.Reader // 用于读取输入
	ch     rune          // 当前字符
}

// NewLexer 创建一个新的 词法分析器
// 初始化 reader 并读取第一个字符
func NewLexer(r io.Reader) *Lexer {
	l := &Lexer{
		reader: bufio.NewReader(r),
	}
	l.readChar()
	return l
}

// 读取字符
func (l *Lexer) readChar() {
	ch, _, err := l.reader.ReadRune()
	if err != nil {
		l.ch = 0 // 遇到错误或EOF时设置为0
	} else {
		l.ch = ch
	}
}

// NextToken 获取下一个词法单元
// 识别并返回下一个标记
// 处理各种类型的标记：运算符、分隔符、标识符、数字、字符串等
func (l *Lexer) NextToken() Token {
	var tok Token
	// 跳过空白字符
	l.skipWhitespace()

	switch l.ch {
	case '=':
		tok = Token{Type: EQ, Literal: "="}
	case '>':
		tok = Token{Type: GT, Literal: ">"}
	case '<':
		tok = Token{Type: LT, Literal: "<"}
	case ',':
		tok = Token{Type: COMMA, Literal: ","}
	case ';':
		tok = Token{Type: SEMI, Literal: ";"}
	case '(':
		tok = Token{Type: LPAREN, Literal: "("}
	case ')':
		tok = Token{Type: RPAREN, Literal: ")"}
	case '*':
		tok = Token{Type: ASTERISK, Literal: "*"}
	case '\'':
		tok.Type = STRING
		// 读取字符串字面量
		tok.Literal = l.readString()
		return tok
	case 0:
		tok = Token{Type: EOF, Literal: ""}
	default:
		if isLetter(l.ch) {
			// 读取标识符（表名、列名等）
			tok.Literal = l.readIdentifier()
			// 将读取到的标识符转换为对应的标记类型(转换为对应tokenType)
			tok.Type = l.lookupIdentifier(tok.Literal)
			return tok
		} else if isDigit(l.ch) {
			tok.Type = INT_LIT
			// 读取数字
			tok.Literal = l.readNumber()
			return tok
		} else {
			tok = Token{Type: ERROR, Literal: string(l.ch)}
		}
	}

	l.readChar()
	return tok
}

func (l *Lexer) skipWhitespace() {
	for unicode.IsSpace(l.ch) {
		l.readChar()
	}
}

// 读取标识符，如：列名、表名
func (l *Lexer) readIdentifier() string {
	var ident bytes.Buffer
	for isLetter(l.ch) || isDigit(l.ch) {
		ident.WriteRune(l.ch)
		l.readChar()
	}
	return ident.String()
}

func (l *Lexer) readNumber() string {
	var num bytes.Buffer
	for isDigit(l.ch) {
		num.WriteRune(l.ch)
		l.readChar()
	}
	return num.String()
}

// 读取字符串字面量
func (l *Lexer) readString() string {
	var str bytes.Buffer
	l.readChar() // 跳过开始的引号
	for l.ch != '\'' && l.ch != 0 {
		str.WriteRune(l.ch)
		l.readChar()
	}
	l.readChar() // 跳过结束的引号
	return str.String()
}

func (l *Lexer) peekChar() rune {
	ch, _, err := l.reader.ReadRune()
	if err != nil {
		return 0
	}
	l.reader.UnreadRune()
	return ch
}

// lookupIdentifier 查找标识符类型
// 将标识符转换为对应的标记类型
// 识别 SQL 关键字
func (l *Lexer) lookupIdentifier(ident string) TokenType {
	switch strings.ToUpper(ident) {
	case "SELECT":
		return SELECT
	case "FROM":
		return FROM
	case "WHERE":
		return WHERE
	case "CREATE":
		return CREATE
	case "TABLE":
		return TABLE
	case "INSERT":
		return INSERT
	case "INTO":
		return INTO
	case "VALUES":
		return VALUES
	case "UPDATE":
		return UPDATE
	case "SET":
		return SET
	case "DELETE":
		return DELETE
	case "DROP":
		return DROP
	case "PRIMARY":
		return PRIMARY
	case "KEY":
		return KEY
	case "INT":
		return INT
	case "TEXT":
		return TEXT
	case "LIKE":
		return LIKE
	default:
		return IDENT
	}
}

// 判断字符是否为字母或下划线
func isLetter(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_'
}

// 判断字符是否为数字
func isDigit(ch rune) bool {
	return unicode.IsDigit(ch)
}
