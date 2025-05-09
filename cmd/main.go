package main

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"os"
	"strings"
	"ziyi.db.com/internal/ast"
	"ziyi.db.com/internal/lexer"
	"ziyi.db.com/internal/parser"
	"ziyi.db.com/internal/storage"
)

var history []string               // 存储命令历史
var backend *storage.MemoryBackend // 存储引擎实例
var historyIndex int               // 当前历史记录索引

// 处理用户输入的命令
func executor(t string) {
	t = strings.TrimSpace(t)
	if t == "" {
		return
	}

	// 添加到历史记录
	history = append(history, t)
	historyIndex = len(history) // 重置历史记录索引

	// 处理退出命令
	if strings.ToLower(t) == "exit" {
		fmt.Println("Bye!")
		os.Exit(0)
	}

	// 创建词法分析器
	l := lexer.NewLexer(strings.NewReader(t))

	// 创建语法分析器
	p := parser.NewParser(l)

	// 解析SQL语句
	stmt, err := p.ParseProgram()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	// 执行SQL语句
	for _, statement := range stmt.Statements {
		switch s := statement.(type) {
		case *ast.CreateTableStatement:
			if err := backend.CreateTable(s); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Table created successfully")
			}
		case *ast.InsertStatement:
			if err := backend.Insert(s); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("1 row inserted")
			}
		case *ast.SelectStatement:
			results, err := backend.Select(s)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
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
		case *ast.UpdateStatement:
			if err := backend.Update(s); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Query OK")
			}
		case *ast.DeleteStatement:
			if err := backend.Delete(s); err != nil {
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

// 提供命令补全功能
func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func main() {
	// 初始化存储引擎
	backend = storage.NewMemoryBackend()
	historyIndex = 0

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
