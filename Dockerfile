# Dockerfile
FROM golang:1.23.8-alpine AS builder

# 设置ZiyiDB工作目录
WORKDIR /app

# 复制go mod和sum文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 编译服务端程序
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ziyidb cmd/main.go

# 编译客户端程序
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ziyidb-cli cmd/client.go

# 使用基础镜像
FROM alpine:latest

# 安装ca-certificates
RUN apk --no-cache add ca-certificates

# 设置工作目录
WORKDIR /root/

# 从builder阶段复制编译好的程序
COPY --from=builder /app/ziyidb .
COPY --from=builder /app/ziyidb-cli .

# 复制配置文件
COPY --from=builder /app/config.json .

# 暴露默认端口
EXPOSE 3118

# 启动服务器模式
ENTRYPOINT ["./ziyidb", "server", "-port=3118"]
