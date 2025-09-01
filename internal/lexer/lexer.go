// internal/lexer/lexer.go
package lexer

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"time"
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

// 操作符映射表
var operatorMap = map[string]TokenType{
	"=":  EQ,
	">":  GT,
	"<":  LT,
	">=": GTE,
	"<=": LTE,
	"!=": NEQ,
}

// NextToken 获取下一个词法单元
func (l *Lexer) NextToken() Token {
	var tok Token
	// 跳过空白字符
	l.skipWhitespace()
	// 检查是否为注释
	if l.ch == '-' && l.peekChar() == '-' {
		return l.readComment()
	}

	switch l.ch {
	case '=', '>', '<', '!':
		// 处理操作符
		tok = l.readOperator()
		return tok
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
		// 检查是否为时间格式
		if _, err := time.Parse("2006-01-02 15:04:05", tok.Literal); err == nil {
			tok.Type = DATETIME
		}
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
		} else if isDigit(l.ch) || l.ch == '.' { // 支持以小数点开头的浮点数（如.123）
			num := l.readNumber()
			if strings.Contains(num, ".") {
				tok.Type = FLOAT
			} else {
				tok.Type = INT
			}
			tok.Literal = num
			return tok
		} else if isTime(l.ch) {
			timeValue := l.readString()
			tok.Literal = timeValue
			tok.Type = DATETIME
		} else {
			tok = Token{Type: ERROR, Literal: string(l.ch)}
		}
	}

	l.readChar()
	return tok
}

// readOperator 读取操作符
func (l *Lexer) readOperator() Token {
	var op bytes.Buffer
	op.WriteRune(l.ch)

	// 检查是否为双字符操作符
	if l.peekChar() == '=' {
		op.WriteRune(l.peekChar())
		l.readChar() // 消费 '='
	}

	literal := op.String()
	if tokenType, exists := operatorMap[literal]; exists {
		l.readChar() // 消费当前字符
		return Token{Type: tokenType, Literal: literal}
	}

	// 如果不是有效的操作符，回退并返回错误
	l.readChar()
	return Token{Type: ERROR, Literal: literal}
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

// 读取数字：支持对浮点数的读取
func (l *Lexer) readNumber() string {
	var num bytes.Buffer
	hasDecimal := false
	for (isDigit(l.ch) || (l.ch == '.' && !hasDecimal)) && l.ch != 0 {
		if l.ch == '.' {
			hasDecimal = true
		}
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
	case "FLOAT":
		return FLOAT
	case "DATETIME":
		return DATETIME
	case "LIKE":
		return LIKE
	case "BETWEEN":
		return BETWEEN
	case "AND":
		return AND
	case "DEFAULT":
		return DEFAULT
	case "GROUP":
		return GROUP
	case "BY":
		return BY
	case "ORDER":
		return ORDER
	case "ASC":
		return ASC
	case "DESC":
		return DESC
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

// 判断是否是时间格式
func isTime(ch rune) bool {
	_, err := time.Parse("2006-01-02 15:04:05", string(ch))
	return err == nil
}

// 读取注释内容
func (l *Lexer) readComment() Token {
	var comment bytes.Buffer
	// 跳过两个减号，即：注释符
	l.readChar()
	l.readChar()
	// 读取直到行尾
	for l.ch != '\n' && l.ch != 0 && l.ch != '\r' {
		comment.WriteRune(l.ch)
		l.readChar()
	}
	return Token{Type: COMMENT, Literal: comment.String()}
}

// 查看下一个字符但不移动指针
func (l *Lexer) peekChar() rune {
	ch, _, err := l.reader.ReadRune()
	if err != nil {
		return 0
	}
	// 回退字符
	_ = l.reader.UnreadRune()
	return ch
}
