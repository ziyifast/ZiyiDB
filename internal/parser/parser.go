package parser

import (
	"fmt"
	"strconv"
	"time"
	"ziyi.db.com/internal/ast"
	"ziyi.db.com/internal/lexer"
)

// Parser 表示语法分析器
// 维护当前和下一个标记，实现向前查看（lookahead）
// 记录解析过程中的错误
type Parser struct {
	l         *lexer.Lexer // 词法分析器
	curToken  lexer.Token  // 当前标记
	peekToken lexer.Token  // 下一个标记
	errors    []string     // 错误信息
}

// NewParser 创建新的语法分析器
// 初始化解析器
// 预读两个标记
func NewParser(l *lexer.Lexer) *Parser {
	p := &Parser{
		l:      l,
		errors: []string{},
	}

	// 读取两个token，设置curToken和peekToken
	p.nextToken()
	p.nextToken()

	return p
}

// nextToken 移动到下一个词法单元
func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

// ParseProgram 解析整个程序
// 解析整个 SQL 程序
// 循环解析每个语句直到结束
func (p *Parser) ParseProgram() (*ast.Program, error) {
	program := &ast.Program{
		Statements: []ast.Statement{},
	}

	for p.curToken.Type != lexer.EOF {
		//跳过注释
		if p.curToken.Type == lexer.COMMENT {
			p.nextToken()
			continue
		}
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if stmt != nil {
			program.Statements = append(program.Statements, stmt)
		}
		p.nextToken()
	}

	return program, nil
}

// parseStatement 解析语句
// 根据当前标记类型选择相应的解析方法
func (p *Parser) parseStatement() (ast.Statement, error) {
	// 跳过注释
	for p.curToken.Type == lexer.COMMENT {
		p.nextToken()
	}
	switch p.curToken.Type {
	case lexer.CREATE:
		return p.parseCreateTableStatement()
	case lexer.INSERT:
		return p.parseInsertStatement()
	case lexer.SELECT:
		return p.parseSelectStatement()
	case lexer.UPDATE:
		return p.parseUpdateStatement()
	case lexer.DELETE:
		return p.parseDeleteStatement()
	case lexer.DROP:
		return p.parseDropTableStatement()
	case lexer.SEMI:
		return nil, nil
	default:
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Type)
	}
}

// parseCreateTableStatement 解析CREATE TABLE语句
// 解析表名
// 解析列定义
// 处理主键约束
func (p *Parser) parseCreateTableStatement() (*ast.CreateTableStatement, error) {
	stmt := &ast.CreateTableStatement{Token: p.curToken}

	if !p.expectPeek(lexer.TABLE) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	// 解析列定义
	for !p.peekTokenIs(lexer.RPAREN) {
		p.nextToken()

		if !p.curTokenIs(lexer.IDENT) {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
		}

		col := ast.ColumnDefinition{
			Name: p.curToken.Literal,
		}

		if !p.expectPeek(lexer.INT) &&
			!p.expectPeek(lexer.TEXT) &&
			!p.expectPeek(lexer.FLOAT) &&
			!p.expectPeek(lexer.DATETIME) {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
		}
		col.Type = string(p.curToken.Type)

		if p.peekTokenIs(lexer.PRIMARY) {
			p.nextToken()
			if !p.expectPeek(lexer.KEY) {
				return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
			}
			col.Primary = true
		}

		if p.peekTokenIs(lexer.DEFAULT) {
			p.nextToken() // 消费 DEFAULT 关键字
			p.nextToken() // 移动到默认值表达式开始位置

			// 解析复杂默认值表达式（支持函数调用、数学表达式等）
			defaultValue, err := p.parseExpression()
			if err != nil {
				return nil, fmt.Errorf("Invalid default value for column '%s': %v", col.Name, err)
			}

			// 创建 DefaultExpression 节点（需要同步修改 AST 定义）
			col.Default = &ast.DefaultExpression{
				Token: p.curToken, // 记录 DEFAULT 关键字的 token 位置
				Value: defaultValue,
			}
			//stmt.Columns = append(stmt.Columns, col)
			//// 向前查看是否还有其他列属性
			//if p.peekTokenIs(lexer.COMMA) || p.peekTokenIs(lexer.RPAREN) {
			//	continue
			//}
		}

		stmt.Columns = append(stmt.Columns, col)

		if p.peekTokenIs(lexer.COMMA) {
			p.nextToken()
		}
	}

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	return stmt, nil
}

// parseInsertStatement 解析INSERT语句
// 解析表名
// 解析 VALUES 子句
// 解析插入的值
func (p *Parser) parseInsertStatement() (*ast.InsertStatement, error) {
	stmt := &ast.InsertStatement{Token: p.curToken}

	if !p.expectPeek(lexer.INTO) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	// 解析可选的列名列表（例如：INSERT INTO users (id, name) VALUES (...)）
	if p.peekTokenIs(lexer.LPAREN) {
		p.nextToken() // 消费左括号
		for !p.peekTokenIs(lexer.RPAREN) {
			p.nextToken()
			if !p.curTokenIs(lexer.IDENT) {
				return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
			}
			// 列名作为标识符表达式存储
			stmt.Columns = append(stmt.Columns, &ast.Identifier{
				Token: p.curToken,
				Value: p.curToken.Literal,
			})
			if p.peekTokenIs(lexer.COMMA) {
				p.nextToken() // 消费逗号分隔符
			}
		}
		if !p.expectPeek(lexer.RPAREN) { // 消费右括号
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
		}
	}

	// 解析 VALUES 关键字
	if !p.expectPeek(lexer.VALUES) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	// 解析值列表（与原逻辑一致）
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	for !p.peekTokenIs(lexer.RPAREN) {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, expr)
		if p.peekTokenIs(lexer.COMMA) {
			p.nextToken()
		}
	}

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	return stmt, nil
}

// parseSelectStatement 解析SELECT语句
// 解析选择列表
// 解析 FROM 子句
// 解析 WHERE 子句
func (p *Parser) parseSelectStatement() (*ast.SelectStatement, error) {
	stmt := &ast.SelectStatement{Token: p.curToken}

	// 解析选择列表
	for !p.peekTokenIs(lexer.FROM) {
		p.nextToken()

		if p.curToken.Type == lexer.ASTERISK {
			stmt.Fields = append(stmt.Fields, &ast.StarExpression{})
			break
		}

		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Fields = append(stmt.Fields, expr)

		if p.peekTokenIs(lexer.COMMA) {
			p.nextToken()
		}
	}

	if !p.expectPeek(lexer.FROM) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	// 解析WHERE子句
	if p.peekTokenIs(lexer.WHERE) {
		p.nextToken()
		p.nextToken()

		// 解析左操作数（列名）
		if !p.curTokenIs(lexer.IDENT) {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
		}
		left := &ast.Identifier{
			Token: p.curToken,
			Value: p.curToken.Literal,
		}

		// 解析操作符
		p.nextToken()
		operator := p.curToken

		// 处理LIKE操作符
		if p.curTokenIs(lexer.LIKE) {
			p.nextToken()
			if !p.curTokenIs(lexer.STRING) {
				return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
			}
			// 移除字符串字面量的引号
			pattern := p.curToken.Literal
			if len(pattern) >= 2 && (pattern[0] == '\'' || pattern[0] == '"') {
				pattern = pattern[1 : len(pattern)-1]
			}
			stmt.Where = &ast.LikeExpression{
				Token:   operator,
				Left:    left,
				Pattern: pattern,
			}
			return stmt, nil
		}

		// 处理BETWEEN操作符
		if p.curTokenIs(lexer.BETWEEN) {
			expr, err := p.parseBetweenExpression(left)
			if err != nil {
				return nil, err
			}
			stmt.Where = expr
			return stmt, nil
		}

		// 处理其他操作符,如：>=、<=、<、>、=、!=、like...
		if !p.isBasicOperator() {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", operator.Type)
		}

		// 解析右操作数
		p.nextToken()
		right, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Where = &ast.BinaryExpression{
			Token:    operator,
			Left:     left,
			Operator: operator.Literal,
			Right:    right,
		}
	}

	return stmt, nil
}

// parseUpdateStatement 解析UPDATE语句
// 解析表名
// 解析 SET 子句
// 解析 WHERE 子句
func (p *Parser) parseUpdateStatement() (*ast.UpdateStatement, error) {
	stmt := &ast.UpdateStatement{Token: p.curToken}

	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(lexer.SET) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	// 解析SET子句
	for {
		p.nextToken()
		if !p.curTokenIs(lexer.IDENT) {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
		}
		column := p.curToken.Literal

		if !p.expectPeek(lexer.EQ) {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
		}

		p.nextToken()
		value, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Set = append(stmt.Set, ast.SetClause{
			Column: column,
			Value:  value,
		})

		if !p.peekTokenIs(lexer.COMMA) {
			break
		}
		p.nextToken()
	}

	// 解析WHERE子句
	if p.peekTokenIs(lexer.WHERE) {
		p.nextToken()
		p.nextToken()

		// 解析左操作数（列名）
		if !p.curTokenIs(lexer.IDENT) {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
		}
		left := &ast.Identifier{
			Token: p.curToken,
			Value: p.curToken.Literal,
		}

		// 解析操作符
		p.nextToken()
		operator := p.curToken

		// 处理LIKE操作符
		if p.curTokenIs(lexer.LIKE) {
			p.nextToken()
			if !p.curTokenIs(lexer.STRING) {
				return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
			}
			// 移除字符串字面量的引号
			pattern := p.curToken.Literal
			if len(pattern) >= 2 && (pattern[0] == '\'' || pattern[0] == '"') {
				pattern = pattern[1 : len(pattern)-1]
			}
			stmt.Where = &ast.LikeExpression{
				Token:   operator,
				Left:    left,
				Pattern: pattern,
			}
			return stmt, nil
		}

		// 处理BETWEEN操作符
		if p.curTokenIs(lexer.BETWEEN) {
			expr, err := p.parseBetweenExpression(left)
			if err != nil {
				return nil, err
			}
			stmt.Where = expr
			return stmt, nil
		}

		// 处理其他操作符,如：>=、<=、<、>、=、!=、like...
		if !p.isBasicOperator() {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", operator.Type)
		}

		// 解析右操作数
		p.nextToken()
		right, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Where = &ast.BinaryExpression{
			Token:    operator,
			Left:     left,
			Operator: operator.Literal,
			Right:    right,
		}
	}

	return stmt, nil
}

// parseDeleteStatement 解析DELETE语句
// 解析表名
// 解析 WHERE 子句
func (p *Parser) parseDeleteStatement() (*ast.DeleteStatement, error) {
	stmt := &ast.DeleteStatement{Token: p.curToken}

	if !p.expectPeek(lexer.FROM) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	// 解析WHERE子句
	if p.peekTokenIs(lexer.WHERE) {
		p.nextToken()
		p.nextToken()

		// 解析左操作数（列名）
		if !p.curTokenIs(lexer.IDENT) {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
		}
		left := &ast.Identifier{
			Token: p.curToken,
			Value: p.curToken.Literal,
		}
		// 解析操作符
		p.nextToken()
		operator := p.curToken

		// 处理LIKE操作符
		if p.curTokenIs(lexer.LIKE) {
			p.nextToken()
			if !p.curTokenIs(lexer.STRING) {
				return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
			}
			// 移除字符串字面量的引号
			pattern := p.curToken.Literal
			if len(pattern) >= 2 && (pattern[0] == '\'' || pattern[0] == '"') {
				pattern = pattern[1 : len(pattern)-1]
			}
			stmt.Where = &ast.LikeExpression{
				Token:   operator,
				Left:    left,
				Pattern: pattern,
			}
			return stmt, nil
		}

		// 处理BETWEEN操作符
		if p.curTokenIs(lexer.BETWEEN) {
			expr, err := p.parseBetweenExpression(left)
			if err != nil {
				return nil, err
			}
			stmt.Where = expr
			return stmt, nil
		}

		// 处理其他操作符,如：>=、<=、<、>、=、!=、like...
		if !p.isBasicOperator() {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", operator.Type)
		}

		// 解析右操作数
		p.nextToken()
		right, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Where = &ast.BinaryExpression{
			Token:    operator,
			Left:     left,
			Operator: operator.Literal,
			Right:    right,
		}
	}

	return stmt, nil
}

// parseDropTableStatement 解析DROP TABLE语句
func (p *Parser) parseDropTableStatement() (*ast.DropTableStatement, error) {
	stmt := &ast.DropTableStatement{Token: p.curToken}

	if !p.expectPeek(lexer.TABLE) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}

	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.peekToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	return stmt, nil
}

// parseExpression 解析表达式(字面量int、string类型，标识符列名、表名等)
// 解析各种类型的表达式
// 支持字面量、标识符等
func (p *Parser) parseExpression() (ast.Expression, error) {
	switch p.curToken.Type {
	case lexer.LPAREN:
		return p.parseGroupedExpression()
	case lexer.INT:
		val, err := strconv.ParseInt(p.curToken.Literal, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Incorrect integer value: '%s'", p.curToken.Literal)
		}
		return &ast.IntegerLiteral{
			BaseExpression: ast.BaseExpression{Token: p.curToken},
			Token:          p.curToken,
			Value:          int32(val),
		}, nil
	case lexer.FLOAT:
		val, err := strconv.ParseFloat(p.curToken.Literal, 32)
		if err != nil {
			return nil, fmt.Errorf("Incorrect float value: '%s'", p.curToken.Literal)
		}
		return &ast.FloatLiteral{
			BaseExpression: ast.BaseExpression{Token: p.curToken},
			Token:          p.curToken,
			Value:          float32(val),
		}, nil
	case lexer.DATETIME:
		val, err := time.Parse("2006-01-02 15:04:05", p.curToken.Literal)
		if err != nil {
			return nil, fmt.Errorf("Incorrect datetime value: '%s'", p.curToken.Literal)
		}
		return &ast.DateTimeLiteral{
			BaseExpression: ast.BaseExpression{Token: p.curToken},
			Token:          p.curToken,
			Value:          val,
		}, nil
	case lexer.STRING:
		return &ast.StringLiteral{
			BaseExpression: ast.BaseExpression{Token: p.curToken},
			Token:          p.curToken,
			Value:          p.curToken.Literal,
		}, nil
	case lexer.ASTERISK:
		return &ast.StarExpression{}, nil
	case lexer.IDENT:
		if p.peekTokenIs(lexer.LPAREN) {
			return p.parseFunctionCall()
		}
		return &ast.Identifier{
			Token: p.curToken,
			Value: p.curToken.Literal,
		}, nil
	default:
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Type)
	}
}

// curTokenIs 检查当前token是否为指定类型
func (p *Parser) curTokenIs(t lexer.TokenType) bool {
	return p.curToken.Type == t
}

// peekTokenIs 检查下一个token是否为指定类型
func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

// expectPeek 检查下一个词法单元是否为预期类型
func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	return false
}

// parseWhereClause 解析WHERE子句
func (p *Parser) parseWhereClause() (ast.Expression, error) {
	p.nextToken()

	// 解析左操作数（列名）
	if !p.curTokenIs(lexer.IDENT) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
	}
	left := &ast.Identifier{
		Token: p.curToken,
		Value: p.curToken.Literal,
	}
	if p.peekTokenIs(lexer.BETWEEN) {
		return p.parseBetweenExpression(left)
	}

	// 解析操作符
	p.nextToken()
	operator := p.curToken

	// 处理LIKE操作符
	if p.curTokenIs(lexer.LIKE) {
		p.nextToken()
		if !p.curTokenIs(lexer.STRING) {
			return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", p.curToken.Literal)
		}
		// 移除字符串字面量的引号
		pattern := p.curToken.Literal
		if len(pattern) >= 2 && (pattern[0] == '\'' || pattern[0] == '"') {
			pattern = pattern[1 : len(pattern)-1]
		}
		return &ast.LikeExpression{
			Token:   operator,
			Left:    left,
			Pattern: pattern,
		}, nil
	}

	// 处理其他操作符
	if !p.curTokenIs(lexer.EQ) && !p.curTokenIs(lexer.GT) && !p.curTokenIs(lexer.LT) {
		return nil, fmt.Errorf("You have an error in your SQL syntax; check the manual that corresponds to your db server version for the right syntax to use near '%s'", operator.Type)
	}

	// 解析右操作数
	p.nextToken()
	right, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	return &ast.BinaryExpression{
		Token:    operator,
		Left:     left,
		Operator: operator.Literal,
		Right:    right,
	}, nil
}

func (p *Parser) parseBetweenExpression(left ast.Expression) (ast.Expression, error) {
	expr := &ast.BetweenExpression{
		Token: p.curToken,
		Left:  left,
	}

	p.nextToken() // 跳过BETWEEN

	// 解析下限值
	lower, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	expr.Low = lower

	if !p.expectPeek(lexer.AND) {
		return nil, fmt.Errorf("expected AND after BETWEEN expression")
	}

	p.nextToken() // 跳过AND

	// 解析上限值
	upper, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	expr.High = upper

	return expr, nil
}

// 新增分组表达式解析
func (p *Parser) parseGroupedExpression() (ast.Expression, error) {
	p.nextToken() // 跳过左括号
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("Missing closing parenthesis")
	}
	return expr, nil
}

// 新增函数调用解析
func (p *Parser) parseFunctionCall() (ast.Expression, error) {
	fn := &ast.FunctionCall{
		Token:  p.curToken, // 当前token是函数名
		Name:   p.curToken.Literal,
		Params: []ast.Expression{},
	}

	// 检查下一个token是否为左括号
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected ( after function name")
	}

	// 如果是右括号，说明没有参数
	if p.peekTokenIs(lexer.RPAREN) {
		p.nextToken() // 消费右括号
		return fn, nil
	}

	// 解析参数列表
	for !p.peekTokenIs(lexer.RPAREN) {
		p.nextToken() // 移动到参数
		param, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		fn.Params = append(fn.Params, param)

		if p.peekTokenIs(lexer.COMMA) {
			p.nextToken() // 消费逗号
		} else if !p.peekTokenIs(lexer.RPAREN) {
			return nil, fmt.Errorf("expected comma or closing parenthesis in function call")
		}
	}

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("Missing closing parenthesis for function call")
	}

	return fn, nil
}

// isBasicOperator 检查是否为操作符，比如 =、>、<、>=、<=、!=、like等
func (p *Parser) isBasicOperator() bool {
	return p.curTokenIs(lexer.EQ) ||
		p.curTokenIs(lexer.GT) ||
		p.curTokenIs(lexer.LT) ||
		p.curTokenIs(lexer.GTE) ||
		p.curTokenIs(lexer.LTE) ||
		p.curTokenIs(lexer.NEQ) ||
		p.curTokenIs(lexer.LIKE)
}
