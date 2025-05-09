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



