// cmd/client.go
package main

import (
	"bufio"
	"fmt"
	"github.com/c-bata/go-prompt"
	"net"
	"os"
	"strings"
	"sync"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: client <server_address:port>")
		os.Exit(1)
	}

	addr := os.Args[1]
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to connect to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// 创建一个用于存储历史命令的切片
	history := []string{}

	// 用于同步接收服务器响应的通道
	responseChan := make(chan string, 100)
	var wg sync.WaitGroup

	// 启动goroutine接收服务器消息
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			responseChan <- scanner.Text()
		}
		close(responseChan)
	}()

	// 启动另一个goroutine处理并打印响应
	wg.Add(1)
	go func() {
		defer wg.Done()
		for response := range responseChan {
			fmt.Println(response)
		}
	}()

	// 先接收服务器的欢迎信息
	// 等待一段时间让服务器发送欢迎信息
	fmt.Println("Connected to ZiyiDB server.")

	// 使用 go-prompt 实现交互式客户端
	p := prompt.New(
		func(in string) {
			input := strings.TrimSpace(in)
			if input == "" {
				return
			}

			// 添加到历史记录
			history = append(history, input)

			// 发送命令到服务器
			_, err := conn.Write([]byte(input + "\n"))
			if err != nil {
				fmt.Printf("Failed to send command: %v\n", err)
				return
			}

			// 特殊处理退出命令
			if input == "exit" || input == "quit" {
				// 关闭连接并等待goroutine结束
				conn.Close()
				wg.Wait()
				os.Exit(0)
			}
		},
		func(d prompt.Document) []prompt.Suggest {
			return []prompt.Suggest{}
		},
		prompt.OptionTitle("ZiyiDB Client"),
		prompt.OptionPrefix("ziyidb> "),
		prompt.OptionHistory(history),
		prompt.OptionLivePrefix(func() (string, bool) {
			return "ziyidb> ", true
		}),
	)
	p.Run()

	// 等待所有goroutine结束
	wg.Wait()
}
