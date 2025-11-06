# ZiyiDB: 从零实现一个简单的 SQL 数据库

ZiyiDB 是一个用 Go 语言实现的简单 SQL 数据库，支持基本的 SQL 操作。本教程将带你从零开始，一步步实现一个功能完整的 SQL 数据库。

## 功能特性

- 支持基本的 SQL 语句：CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE
- 支持 WHERE 子句和基本的条件表达式
- 支持 LIKE 操作符进行模式匹配
- 交互式命令行界面，支持命令历史
- 内存存储引擎

## 项目结构

```
ziyi-db/
├── cmd/
│   └── main.go          # 主程序入口
├── internal/
│   ├── ast/            # 抽象语法树定义
│   ├── lexer/          # 词法分析器
│   ├── parser/         # 语法分析器
│   └── storage/        # 存储引擎
├── go.mod
└── README.md
```

## 教程目录

1. [词法分析器 (Lexer)](#1-词法分析器-lexer)
2. [抽象语法树 (AST)](#2-抽象语法树-ast)
3. [语法分析器 (Parser)](#3-语法分析器-parser)
4. [存储引擎 (Storage)](#4-存储引擎-storage)
5. [REPL 交互界面](#5-repl-交互界面)

## 1. 词法分析器 (Lexer)

词法分析器负责将输入的 SQL 语句分解成一系列标记（Token）。每个标记代表一个语法单元，如关键字、标识符、运算符等。

### 1.1 定义标记类型

新建ziyi-db/internal/lexer/token.go文件,完成词法分析器（Lexer）中的标记（Token）定义部分，用于将 SQL 语句分解成基本的语法单元。
定义词法单元以及关键字
- 包含常见的SQL关键字，如：select、update等
- 包含符号关键字：=、>、<
- 包含字段类型：INT、字符型
- 包含标识符：INDENT，解析出来的SQL列名、表名
```go
type TokenType string

const (
    SELECT  TokenType = "SELECT"
    FROM    TokenType = "FROM"
    IDENT   TokenType = "IDENT"  // 标识符（如列名、表名）
    INT_LIT TokenType = "INT"    // 整数字面量
    STRING  TokenType = "STRING" // 字符串字面量
    EQ TokenType = "=" // 等于
    GT TokenType = ">" // 大于
    LT TokenType = "<" // 小于
    ....
)

// Token 词法单元
// Type：标记的类型（如 SELECT、IDENT 等）
// Literal：标记的实际值（如具体的列名、数字等）
type Token struct {
    Type    TokenType // 标记类型
    Literal string    // 标记的实际值
}
```

### 1.2 实现词法分析器
新建ziyi-db/internal/lexer/lexer.go文件，这是词法分析器（Lexer）的核心实现，负责将输入的 SQL 语句分解成标记（Token）序列。
读取SQL到内存中并进行解析，将字符转换为对应关键字


```go
/**

示例：SELECT id, name FROM users WHERE age > 18;

处理过程：
跳过空白字符
读取 "SELECT" 并识别为关键字
读取 "id" 并识别为标识符
读取 "," 并识别为分隔符
读取 "name" 并识别为标识符
读取 "FROM" 并识别为关键字
读取 "users" 并识别为标识符
读取 "WHERE" 并识别为关键字
读取 "age" 并识别为标识符
读取 ">" 并识别为运算符
读取 "18" 并识别为数字
读取 ";" 并识别为分隔符
这个词法分析器是 SQL 解析器的第一步，它将输入的 SQL 语句分解成标记序列，为后续的语法分析提供基础

该SQL 语句会被词法分析器分解成以下标记序列：
{Type: SELECT, Literal: "SELECT"}
{Type: IDENT, Literal: "id"}
{Type: COMMA, Literal: ","}
{Type: IDENT, Literal: "name"}
{Type: FROM, Literal: "FROM"}
{Type: IDENT, Literal: "users"}
{Type: WHERE, Literal: "WHERE"}
{Type: IDENT, Literal: "age"}
{Type: GT, Literal: ">"}
{Type: INT_LIT, Literal: "18"}
{Type: SEMI, Literal: ";"}
解析后的标记随后会被传递给语法分析器（Parser）进行进一步处理，构建抽象语法树（AST）。
 */
```

## 2. 抽象语法树 (AST)
> 抽象语法树用于表示 SQL 语句的语法结构。我们需要为每种 SQL 语句定义相应的节点类型。
> 
新建internal/ast/ast.go
构建不同SQL语句的结构，以及查询结果等。
这个 AST 定义文件是 SQL 解析器的核心部分，它：
1. 定义了所有 SQL 语句的语法结构
2. 提供了类型安全的方式来表示 SQL 语句
3. 支持复杂的表达式和条件
4. 便于后续的语义分析和执行
   通过这个 AST，我们可以：
- 验证 SQL 语句的语法正确性
- 进行语义分析
- 生成执行计划
- 执行 SQL 语句

```go
/*
SELECT id, name FROM users WHERE age > 18;

交给语法分析器parser解析后的AST结构为：

SelectStatement
├── Fields
│   ├── Identifier{Value: "id"}
│   └── Identifier{Value: "name"}
├── TableName: "users"
└── Where
    └── BinaryExpression
        ├── Left: Identifier{Value: "age"}
        ├── Operator: ">"
        └── Right: IntegerLiteral{Value: "18"}
 */

```

## 3. 语法分析器 (Parser)
> 语法分析器负责将词法分析器生成的标记序列转换为抽象语法树。

SQL 解析器（Parser）的实现，负责将词法分析器（Lexer）产生的标记（Token）序列转换为抽象语法树（AST）。
语法分析器SQL 数据库系统的关键组件，负责：
- 验证 SQL 语句的语法正确性
- 构建抽象语法树
- 为后续的语义分析和执行提供基础
  新建internal/parser/parser.go。

```sql
CREATE TABLE users (
id INT PRIMARY KEY,
name TEXT
);


解析过程：
1. 识别 CREATE 关键字
2. 解析 TABLE 关键字
3. 解析表名 "users"
4. 解析列定义：
列名 "id"，类型 INT，主键
列名 "name"，类型 TEXT
5. 生成 CREATE TABLE 语句的 AST
```

## 4. 存储引擎 (Storage)

> 存储引擎负责实际的数据存储和检索操作。

这是内存存储引擎的实现，负责处理 SQL 语句的实际执行和数据存储。
本期存储引擎实现了：
1. 完整的数据操作（CRUD）
2. 主键约束
3. 索引支持
4. 类型检查
5. 条件评估
6. 模式匹配
   它是 SQL 数据库系统的核心组件，负责：
- 数据存储和管理
- 查询执行
- 数据完整性维护
- 性能优化（通过索引）

```sql
-- 创建表
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT
);

-- 插入数据
INSERT INTO users VALUES (1, 'Alice');

-- 查询数据
SELECT * FROM users WHERE name LIKE 'A%';

-- 更新数据
UPDATE users SET name = 'Bob' WHERE id = 1;

-- 删除数据
DELETE FROM users WHERE id = 1;


-- 存储引擎会根据解析后的语法分析器，创建出对应的数据结构（如：在内存中），以及对外暴露对该数据的操作（CRUD）
```

## 5. REPL 交互界面

最后，实现一个交互式的命令行界面，让用户可以输入 SQL 命令并查看结果。
这是 ZiyiDB 的主程序，实现了一个交互式的 SQL 命令行界面。
> 为了实现客户端可以上下翻找之前执行的命令以及cli客户端的美观，我们这里使用"github.com/c-bata/go-prompt"库

主要实现：
1. 交互式命令行界面
2. SQL 命令解析和执行
3. 命令历史记录
4. 查询结果格式化
5. 错误处理和提示


## 使用示例
```sql
-- 1. 创建表
CREATE TABLE users (id INT PRIMARY KEY,name TEXT,age INT);


-- 2. 插入用户数据
INSERT INTO users VALUES (1, 'Alice', 20);
INSERT INTO users VALUES (2, 'Bob', 25);
INSERT INTO users VALUES (3, 'Charlie', 30);
INSERT INTO users VALUES (4, 'David', 35);
INSERT INTO users VALUES (5, 'Eve', 40);

-- 3. 测试主键冲突
INSERT INTO users VALUES (1, 'Tomas', 21);


-- 4. 基本查询测试
-- 4.1 查询所有数据
SELECT * FROM users;


-- 4.2 查询特定列
SELECT id, name FROM users;

-- 5. WHERE 子句测试
-- 比较运算符 -- 待实现 != = >= <=逻辑
SELECT * FROM users WHERE age > 25;
SELECT * FROM users WHERE age < 30;

-- 6. LIKE 操作符测试
insert into users values (6,'Alan',18);
SELECT * FROM users WHERE name LIKE 'A%';  -- 以 A 开头


-- 7. 更新操作测试
-- 7.1 更新单个字段
UPDATE users SET age = 21 WHERE name = 'Alice';

-- 7.2 更新多个字段
UPDATE users SET name = 'Robert', age = 8 WHERE id = 2;


-- 8. 删除操作测试
DELETE FROM users WHERE age > 30;

-- 9. 清理测试数据
DROP TABLE users;

-- 10. 验证表已删除
SELECT * FROM users;    -- 应该失败

```

## 如何运行

1. 克隆仓库：
```bash
git clone https://github.com/ziyifast/ZiyiDB.git
cd ziyi-db
```

2. 安装依赖：
```bash
go mod tidy
```

3. 编译并运行：
```bash
go build -o ZiyiDB cmd/main.go
./ZiyiDB
```

运行效果：
<img width="652" alt="image" src="https://github.com/user-attachments/assets/06ff023d-e536-45ab-9451-2ff74588abb8" />
## 已实现功能
### 支持字段类型
| 字段类型 | 描述   |
| --- |------|
| INT | 整数类型 |
| TEXT | 文本类型 |
| DATETIME | 日期类型 |
| FLOAT | 浮点类型 |

### 支持关键字
| 关键字         | 描述      |
|-------------|---------|
| CREATE      | 创建数据库对象 |
| DROP        | 删除数据库对象 |
| SELECT      | 查询数据    |
| INSERT      | 插入数据    |
| UPDATE      | 更新数据    |
| DELETE      | 删除数据    |
| WHERE       | 条件查询    |

### 支持索引类型
| 索引          | 描述   |
|-------------|------|
| PRIMARY KEY | 主键索引 |

### 其他功能
1. 支持>、=、<比较符号
2. 支持LIKE模糊匹配
3. 支持注释--
4. 支持between and范围查询



## 贡献

🚀欢迎提交 Issue 和 Pull Request！也欢迎大家star⭐️！！！

## 许可证

MIT License

## 后续规划

| 规划            | 是否已完成 |
|---------------|-------|
| 实现!= >= <=等运算符 | ✅     |
| 支持更多数据类型      | ✅     |
| 实现内置函数        | ✅     |
| 支持事务          | ✅     |
| 支持保存数据到文件     | ✅     |
| 支持数据库隔离表      | ✅     |
| 支持用户名密码登录     |       |

...

