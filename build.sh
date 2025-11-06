# 构建镜像
docker build -t ziyidb .

# 创建容器
docker run -d \
  --name ziyidb \
  -p 3118:3118 \
  --restart unless-stopped \
  -v /Users/ziyi/GolandProjects/ZiyiDB/config.json:/root/config.json \
  -v /Users/ziyi/GolandProjects/ZiyiDB/data:/root/data \
  ziyidb


# 连接到存储引擎服务
docker run --rm -it ziyidb bash

# 在容器内执行客户端连接
./ziyidb-cli localhost:3118

# 执行SQL命令测试
CREATE DATABASE test_memory;
USE test_memory;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
INSERT INTO users VALUES (1, 'Alice', 25);
INSERT INTO users VALUES (2, 'Bob', 30);
SELECT * FROM users;

