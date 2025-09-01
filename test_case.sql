---- 第一期 基础功能测试
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
-- 比较运算符 -- 待实现 != >= <=逻辑
SELECT * FROM users WHERE age > 25;
SELECT * FROM users WHERE age < 30;

-- 6. LIKE 操作符测试
insert into users values (6,'Alan',18);
-- 以 A 开头
SELECT * FROM users WHERE name LIKE 'A%';

-- 7. 更新操作测试
-- 7.1 更新单个字段
UPDATE users SET age = 21 WHERE name = 'Alice';

-- 7.2 更新多个字段
UPDATE users SET name = 'Robert', age = 8 WHERE id = 2;


-- 8. 删除操作测试
DELETE FROM users WHERE age > 30;
select * from users;

-- 9. 清理测试数据
DROP TABLE users;

-- 10. 验证表已删除
-- 应该失败
SELECT * FROM users;



---- 第二期 新增对float、datetime字段，以及between and关键字的支持，以及新增对注释符的支持
CREATE TABLE users (id INT PRIMARY KEY,name text,age INT,score FLOAT, ctime DATETIME ); -- 创建表
INSERT INTO users VALUES (1, 'Alice', 20,89.0, '2023-07-01 12:00:00');
INSERT INTO users VALUES (2, 'Bob', 25,98.3, '2023-07-04 12:00:00');
select * from users where age between 21 and 28;
select * from users where score < 90.0;
select * from users where ctime between '2021-07-02 12:00:00' and '2023-07-05 12:00:00';
select * from users where ctime >  '2023-07-02 12:00:00';
drop table users;

---- 第三期 新增对default 默认值的支持
CREATE TABLE users (id INT PRIMARY KEY,name text,age INT DEFAULT 18,score FLOAT, ctime DATETIME DEFAULT '2023-07-04 12:00:00');
INSERT INTO users (id, name, age, score, ctime) VALUES (1, 'Alice', 25, 90.0, '2025-07-04 12:00:00');
INSERT INTO users (id, name, score) VALUES (2, 'Bob',98.3);

INSERT INTO users (id, name, age, score) VALUES (3, 'Bo3', 38,98.3);
select * from users;
drop table users;


---- 第四期 新增对内置函数的支持：count、sum、avg、max、min 2. 新增>=、<=、!=操作符
CREATE TABLE users (id INT PRIMARY KEY,name text,age INT);
INSERT INTO users VALUES (1, 'Alice', 20);
INSERT INTO users VALUES (2, 'Bob', 25);
INSERT INTO users VALUES (3, 'Charlie', 30);
select * from users where age != 25;

select count(*) from users;
select count(*) from users where age <= 25;

select sum(age) from users;
select sum(age) from users where age >= 25;
update users set name = 'tom' where age between 18 and 28;
select avg(age) from users;
select max(age) from users;
select min(age) from users;
delete from users where age between 18 and 28;
drop table users;

---- 第五期 新增group by 、order by 支持
-- group by 实现
CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, category TEXT, amount FLOAT);
INSERT INTO sales VALUES (1, 'Apple', 'Fruit', 10.5);
INSERT INTO sales VALUES (2, 'Banana', 'Fruit', 8.0);
INSERT INTO sales VALUES (3, 'Carrot', 'Vegetable', 5.2);
INSERT INTO sales VALUES (4, 'Broccoli', 'Vegetable', 7.3);
INSERT INTO sales VALUES (5, 'Orange', 'Fruit', 9.8);
SELECT category, COUNT(*) FROM sales GROUP BY category;
SELECT category, SUM(amount) FROM sales GROUP BY category;
SELECT category, AVG(amount) FROM sales GROUP BY category;

-- order by 实现
SELECT * FROM sales ORDER BY amount;
SELECT * FROM sales ORDER BY amount DESC;
SELECT * FROM sales ORDER BY category, amount DESC;

DROP TABLE sales;


