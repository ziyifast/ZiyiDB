// network/server.go
package network

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"ziyi.db.com/internal/ast"
	"ziyi.db.com/internal/lexer"
	"ziyi.db.com/internal/parser"
	"ziyi.db.com/internal/storage"
)

const DefaultPort = "3118"

type Server struct {
	backend     *storage.MemoryBackend
	port        string
	connections map[net.Conn]*serverConnection
	connMutex   sync.RWMutex
}

type serverConnection struct {
	txn *storage.Transaction
	mu  sync.RWMutex
}

func NewServer(backend *storage.MemoryBackend, port string) *Server {
	return &Server{
		backend:     backend,
		port:        port,
		connections: make(map[net.Conn]*serverConnection),
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("ZiyiDB server listening on port %s\n", s.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		// 为每个连接创建连接上下文
		s.connMutex.Lock()
		s.connections[conn] = &serverConnection{}
		s.connMutex.Unlock()

		// 启动goroutine处理连接
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.connMutex.Lock()
		delete(s.connections, conn)
		s.connMutex.Unlock()
	}()

	conn.Write([]byte("Welcome to ZiyiDB!\n"))

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		if input == "exit" || input == "quit" {
			conn.Write([]byte("Bye!\n"))
			break
		}

		// 处理命令
		response := s.executeCommand(conn, input)
		conn.Write([]byte(response + "\n"))
	}
}

func (s *Server) executeCommand(conn net.Conn, command string) string {
	// 获取连接的事务上下文
	s.connMutex.RLock()
	connCtx := s.connections[conn]
	s.connMutex.RUnlock()

	connCtx.mu.Lock()
	defer connCtx.mu.Unlock()

	// 处理事务相关命令
	if strings.HasPrefix(strings.ToLower(command), "begin") {
		if connCtx.txn != nil {
			return "Error: Transaction already active"
		}

		connCtx.txn = s.backend.BeginTransaction()
		return fmt.Sprintf("Transaction %d started", connCtx.txn.ID)
	}

	if strings.HasPrefix(strings.ToLower(command), "commit") {
		if connCtx.txn == nil {
			return "Error: No active transaction"
		}

		err := connCtx.txn.Commit()
		if err != nil {
			return fmt.Sprintf("Error: %v", err)
		}

		txnID := connCtx.txn.ID
		connCtx.txn = nil
		return fmt.Sprintf("Transaction %d committed", txnID)
	}

	if strings.HasPrefix(strings.ToLower(command), "rollback") {
		if connCtx.txn == nil {
			return "Error: No active transaction"
		}

		err := connCtx.txn.Rollback()
		if err != nil {
			return fmt.Sprintf("Error: %v", err)
		}

		txnID := connCtx.txn.ID
		connCtx.txn = nil
		return fmt.Sprintf("Transaction %d rolled back", txnID)
	}

	// 处理SQL命令
	l := lexer.NewLexer(strings.NewReader(command))
	p := parser.NewParser(l)

	parsedStmt, err := p.ParseProgram()
	if err != nil {
		return fmt.Sprintf("Parse error: %v", err)
	}

	var result string
	for _, statement := range parsedStmt.Statements {
		switch stmt := statement.(type) {
		case *ast.CreateTableStatement:
			if err := s.backend.CreateTable(stmt); err != nil {
				result += fmt.Sprintf("Error: %v\n", err)
			} else {
				result += "Table created successfully\n"
			}
		case *ast.InsertStatement:
			if err := s.backend.Insert(stmt, connCtx.txn); err != nil {
				result += fmt.Sprintf("Error: %v\n", err)
			} else {
				result += "1 row inserted\n"
			}
		case *ast.SelectStatement:
			results, err := s.backend.Select(stmt, connCtx.txn)
			if err != nil {
				result += fmt.Sprintf("Error: %v\n", err)
			} else {
				result += formatResults(results) + "\n"
			}
		case *ast.UpdateStatement:
			if err := s.backend.Update(stmt, connCtx.txn); err != nil {
				result += fmt.Sprintf("Error: %v\n", err)
			} else {
				result += "Query OK\n"
			}
		case *ast.DeleteStatement:
			if err := s.backend.Delete(stmt, connCtx.txn); err != nil {
				result += fmt.Sprintf("Error: %v\n", err)
			} else {
				result += "Query OK\n"
			}
		case *ast.DropTableStatement:
			if err := s.backend.DropTable(stmt); err != nil {
				result += fmt.Sprintf("Error: %v\n", err)
			} else {
				result += "Table dropped successfully\n"
			}
		default:
			result += fmt.Sprintf("Unsupported statement type: %T\n", stmt)
		}
	}

	return strings.TrimSpace(result)
}

func formatResults(results *storage.Results) string {
	if len(results.Rows) == 0 {
		return "(0 rows)"
	}

	// 计算每列的最大宽度
	colWidths := make([]int, len(results.Columns))
	for i, col := range results.Columns {
		colWidths[i] = len(col.Name)
	}
	for _, row := range results.Rows {
		for i, cell := range row {
			cellLen := len(cell.String())
			if cellLen > colWidths[i] {
				colWidths[i] = cellLen
			}
		}
	}

	// 构建结果字符串
	var output strings.Builder

	// 打印表头
	output.WriteString("+")
	for _, width := range colWidths {
		output.WriteString(strings.Repeat("-", width+2))
		output.WriteString("+")
	}
	output.WriteString("\n")

	// 打印列名
	output.WriteString("|")
	for i, col := range results.Columns {
		output.WriteString(fmt.Sprintf(" %-*s |", colWidths[i], col.Name))
	}
	output.WriteString("\n")

	// 打印分隔线
	output.WriteString("+")
	for _, width := range colWidths {
		output.WriteString(strings.Repeat("-", width+2))
		output.WriteString("+")
	}
	output.WriteString("\n")

	// 打印数据行
	for _, row := range results.Rows {
		output.WriteString("|")
		for i, cell := range row {
			output.WriteString(fmt.Sprintf(" %-*s |", colWidths[i], cell.String()))
		}
		output.WriteString("\n")
	}

	// 打印底部边框
	output.WriteString("+")
	for _, width := range colWidths {
		output.WriteString(strings.Repeat("-", width+2))
		output.WriteString("+")
	}
	output.WriteString("\n")

	// 打印行数统计
	output.WriteString(fmt.Sprintf("%d rows in set", len(results.Rows)))

	return output.String()
}
