// cmd/main.go
package main

import (
	"flag"
	"fmt"
	"github.com/c-bata/go-prompt"
	"log"
	"os"
	"strings"
	"ziyi.db.com/internal/ast"
	"ziyi.db.com/internal/lexer"
	"ziyi.db.com/internal/parser"
	"ziyi.db.com/internal/storage"
	"ziyi.db.com/network"
)

var history []string                // 存储命令历史
var backend *storage.MemoryBackend  // 存储引擎实例
var currentTxn *storage.Transaction // 当前事务
var historyIndex int                // 当前历史记录索引

// 处理用户输入的命令
func executor(t string) {
	// 分割多个SQL语句（用分号分隔）
	statements := strings.Split(t, ";")

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// 添加到历史记录
		history = append(history, stmt)
		historyIndex = len(history) // 重置历史记录索引

		// 处理退出命令
		if strings.ToLower(stmt) == "exit" {
			fmt.Println("Bye!")
			os.Exit(0)
		}

		// 处理事务相关命令
		if strings.HasPrefix(strings.ToLower(stmt), "begin") {
			currentTxn = backend.BeginTransaction()
			fmt.Printf("Transaction %d started\n", currentTxn.ID)
			continue
		}

		if strings.HasPrefix(strings.ToLower(stmt), "commit") {
			if currentTxn == nil {
				fmt.Println("Error: No active transaction")
				continue
			}
			if err := currentTxn.Commit(); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Transaction %d committed\n", currentTxn.ID)
			}
			currentTxn = nil
			continue
		}

		if strings.HasPrefix(strings.ToLower(stmt), "rollback") {
			if currentTxn == nil {
				fmt.Println("Error: No active transaction")
				continue
			}
			if err := currentTxn.Rollback(); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Transaction %d rolled back\n", currentTxn.ID)
			}
			currentTxn = nil
			continue
		}

		// 创建词法分析器
		l := lexer.NewLexer(strings.NewReader(stmt))

		// 创建语法分析器
		p := parser.NewParser(l)

		// 解析SQL语句
		parsedStmt, err := p.ParseProgram()
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		// 执行SQL语句
		for _, statement := range parsedStmt.Statements {
			switch s := statement.(type) {
			case *ast.CreateTableStatement:
				if err := backend.CreateTable(s); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Table created successfully")
				}
			case *ast.InsertStatement:
				if err := backend.Insert(s, currentTxn); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("1 row inserted")
				}
			case *ast.SelectStatement:
				results, err := backend.Select(s, currentTxn)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					printResults(results)
				}
			case *ast.UpdateStatement:
				if err := backend.Update(s, currentTxn); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Query OK")
				}
			case *ast.DeleteStatement:
				if err := backend.Delete(s, currentTxn); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Query OK")
				}
			case *ast.DropTableStatement:
				if err := backend.DropTable(s); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Table dropped successfully")
				}
			default:
				fmt.Printf("Unsupported statement type: %T\n", s)
			}
		}
	}
}

func printResults(results *storage.Results) {
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

	// 打印表头
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2))
		fmt.Print("+")
	}
	fmt.Println()

	// 打印列名
	fmt.Print("|")
	for i, col := range results.Columns {
		fmt.Printf(" %-*s |", colWidths[i], col.Name)
	}
	fmt.Println()

	// 打印分隔线
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2))
		fmt.Print("+")
	}
	fmt.Println()

	// 打印数据行
	for _, row := range results.Rows {
		fmt.Print("|")
		for i, cell := range row {
			fmt.Printf(" %-*s |", colWidths[i], cell.String())
		}
		fmt.Println()
	}

	// 打印底部边框
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2))
		fmt.Print("+")
	}
	fmt.Println()

	// 打印行数统计
	fmt.Printf("%d rows in set\n", len(results.Rows))
}

// 提供命令补全功能
func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func main() {
	// 添加命令行参数
	port := flag.String("port", "3118", "Port to listen on")
	flag.Parse()

	// 初始化存储引擎
	backend = storage.NewMemoryBackend()

	// 检查是否以服务器模式运行
	args := flag.Args()
	if len(args) > 0 && args[0] == "server" {
		// 启动服务器模式
		// 允许通过环境变量覆盖端口
		portEnv := os.Getenv("ZIYIDB_PORT")
		if portEnv != "" {
			*port = portEnv
		}

		server := network.NewServer(backend, *port)
		fmt.Printf("Starting ZiyiDB server on port %s...\n", *port)
		log.Fatal(server.Start())
	}

	fmt.Println("Welcome to ZiyiDB!")
	fmt.Println("Type your SQL commands (type 'exit' to quit)")

	p := prompt.New(
		executor,
		completer,
		prompt.OptionTitle("ZiyiDB: A Simple SQL Database"),
		prompt.OptionPrefix("ziyidb> "),
		prompt.OptionHistory(history),
		prompt.OptionLivePrefix(func() (string, bool) {
			return "ziyidb> ", true
		}),
		//实现方向键上下翻阅历史命令
		// 上键绑定
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.Up,
			Fn: func(buf *prompt.Buffer) {
				if historyIndex > 0 {
					historyIndex--
					buf.DeleteBeforeCursor(len(buf.Text()))
					buf.InsertText(history[historyIndex], false, true)
				}
			},
		}),
		// 下键绑定
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.Down,
			Fn: func(buf *prompt.Buffer) {
				if historyIndex < len(history)-1 {
					historyIndex++
					buf.DeleteBeforeCursor(len(buf.Text()))
					buf.InsertText(history[historyIndex], false, true)
				} else if historyIndex == len(history)-1 {
					historyIndex++
					buf.DeleteBeforeCursor(len(buf.Text()))
				}
			},
		}),
	)
	p.Run()
}
