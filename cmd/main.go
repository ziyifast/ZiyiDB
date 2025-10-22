// cmd/main.go
package main

import (
	"flag"
	"fmt"
	"github.com/c-bata/go-prompt"
	"log"
	"os"
	"strings"
	"ziyi.db.com/config"
	"ziyi.db.com/internal/ast"
	"ziyi.db.com/internal/lexer"
	"ziyi.db.com/internal/parser"
	"ziyi.db.com/internal/storage"
	"ziyi.db.com/network"
)

// 将原来的变量声明修改为：
var history []string                // 存储命令历史
var backend storage.Engine          // 存储引擎实例（修改类型为接口类型）
var currentTxn *storage.Transaction // 当前事务
var historyIndex int                // 当前历史记录索引
var currentDatabase string          // 当前用户选择的数据库

type dbContextAdapter struct {
	dbName *string
}

func (d *dbContextAdapter) GetDBName() string {
	return *d.dbName
}

func (d *dbContextAdapter) SetDBName(dbName string) {
	*d.dbName = dbName
}

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
			if currentDatabase == "" {
				// 检查是否是非数据库操作语句
				_, isCreateDB := statement.(*ast.CreateDatabaseStatement)
				_, isShowDBs := statement.(*ast.ShowDatabasesStatement)
				_, isDropDB := statement.(*ast.DropDatabaseStatement)
				_, isUseDB := statement.(*ast.UseDatabaseStatement)
				_, isShowTables := statement.(*ast.ShowTablesStatement)
				// 如果不是允许的语句类型，则提示需要选择数据库
				if !isCreateDB && !isShowDBs && !isDropDB && !isUseDB && !isShowTables {
					fmt.Println("No database selected. Use 'USE database_name' to select a database.")
					continue
				}
			}
			switch s := statement.(type) {
			case *ast.CreateDatabaseStatement:
				if err := backend.CreateDatabase(s); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Database created successfully")
				}
			case *ast.DropDatabaseStatement:
				if err := backend.DropDatabase(s); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Database dropped successfully")
				}
			case *ast.ShowDatabasesStatement:
				result := backend.ShowDatabases()
				printResults(result)
			case *ast.ShowTablesStatement:
				result := backend.ShowTables(&dbContextAdapter{&currentDatabase})
				printResults(result)
			case *ast.UseDatabaseStatement:
				if err := backend.UseDatabase(s, &dbContextAdapter{&currentDatabase}); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Printf("Database changed to '%s'\n", currentDatabase)
				}
			case *ast.CreateTableStatement:
				if err := backend.CreateTable(currentDatabase, s); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Table created successfully")
				}
			case *ast.InsertStatement:
				if err := backend.Insert(currentDatabase, s, currentTxn); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("1 row inserted")
				}
			case *ast.SelectStatement:
				results, err := backend.Select(currentDatabase, s, currentTxn)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					printResults(results)
				}
			case *ast.UpdateStatement:
				if err := backend.Update(currentDatabase, s, currentTxn); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Query OK")
				}
			case *ast.DeleteStatement:
				if err := backend.Delete(currentDatabase, s, currentTxn); err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Query OK")
				}
			case *ast.DropTableStatement:
				if err := backend.DropTable(currentDatabase, s); err != nil {
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
	// 添加配置文件参数
	configPath := flag.String("config", "config.json", "Path to config file")
	port := flag.String("port", "3118", "Port to listen on")
	flag.Parse()

	// 加载配置
	config, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("无法加载配置文件: %v\n", err)
		os.Exit(1)
	}

	// 初始化存储引擎（修改这部分）
	switch config.Storage.Type {
	case "memory":
		backend = storage.NewMemoryBackend()
	case "disk":
		backend = storage.NewDiskBackend(config.Storage.DataPath)
	default:
		fmt.Printf("未知的存储引擎类型: %s\n", config.Storage.Type)
		os.Exit(1)
	}

	// 检查是否以服务器模式运行
	args := flag.Args()
	if len(args) > 0 && args[0] == "server" {
		// 启动服务器模式
		portEnv := os.Getenv("ZIYIDB_PORT")
		if portEnv != "" {
			*port = portEnv
		} else if config.Server.Port != "" {
			*port = config.Server.Port
		}

		// 类型断言获取 MemoryBackend（如果使用的是内存引擎）
		if memoryBackend, ok := backend.(*storage.MemoryBackend); ok {
			server := network.NewServer(memoryBackend, *port)
			fmt.Printf("Starting ZiyiDB server on port %s with %s storage...\n", *port, config.Storage.Type)
			log.Fatal(server.Start())
		} else {
			fmt.Println("Server mode only supports memory storage engine")
			os.Exit(1)
		}
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
