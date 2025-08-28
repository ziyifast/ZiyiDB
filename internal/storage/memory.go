// internal/storage/memory.go
package storage

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"ziyi.db.com/internal/ast"
)

// MemoryBackend 内存存储引擎，管理所有表
type MemoryBackend struct {
	tables map[string]*Table
}

// Table 数据表，包含列定义、数据行和索引
type Table struct {
	Name    string
	Columns []ast.ColumnDefinition
	Rows    [][]Cell
	Indexes map[string]*Index // 值到行索引的映射
}

// Index 索引，用于加速查询
type Index struct {
	Column string
	Values map[string][]int // 值到行索引的映射
}

// Results 表示查询结果
type Results struct {
	Columns []ResultColumn
	Rows    [][]Cell
}

// ResultColumn 表示结果列
type ResultColumn struct {
	Name string
	Type string
}

// NewMemoryBackend 创建新的内存存储引擎
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		tables: make(map[string]*Table),
	}
}

// CreateTable 创建表
// 验证表名唯一性
// 创建表结构
// 为主键列创建索引
func (b *MemoryBackend) CreateTable(stmt *ast.CreateTableStatement) error {
	if _, exists := b.tables[stmt.TableName]; exists {
		return fmt.Errorf("Table '%s' already exists", stmt.TableName)
	}

	table := &Table{
		Name:    stmt.TableName,
		Columns: stmt.Columns,
		Rows:    make([][]Cell, 0),
		Indexes: make(map[string]*Index),
	}

	// 为主键创建索引
	for _, col := range stmt.Columns {
		if col.Primary {
			table.Indexes[col.Name] = &Index{
				Column: col.Name,
				Values: make(map[string][]int),
			}
		}
	}

	b.tables[stmt.TableName] = table
	return nil
}

// Insert 插入数据
// 验证表存在性
// 检查数据完整性
// 处理主键约束
// 维护索引
func (b *MemoryBackend) Insert(stmt *ast.InsertStatement) error {
	table, exists := b.tables[stmt.TableName]
	if !exists {
		return fmt.Errorf("Table '%s' doesn't exist", stmt.TableName)
	}
	// 构建列名到表列索引的映射
	colIndexMap := make(map[string]int)
	for idx, col := range table.Columns {
		colIndexMap[col.Name] = idx
	}
	// 初始化行数据（长度为表的总列数）
	row := make([]Cell, len(table.Columns))
	// 处理插入列列表（用户显式指定的列或隐式全列）
	var insertCols []*ast.Identifier
	//用户SQL需要插入的列名、值的映射
	userColMap := make(map[string]ast.Expression)
	if len(stmt.Columns) > 0 {
		insertCols = stmt.Columns
		for i, col := range stmt.Columns {
			userColMap[col.Token.Literal] = stmt.Values[i]
		}
	} else {
		// 未指定列时默认使用表的所有列
		insertCols = make([]*ast.Identifier, len(table.Columns))
		for i, col := range table.Columns {
			insertCols[i] = &ast.Identifier{Value: col.Name}
			userColMap[col.Name] = stmt.Values[i]
		}
	}
	// 检查值数量与指定列数量是否匹配
	if len(stmt.Values) != len(insertCols) {
		return fmt.Errorf("Column count doesn't match value count at row 1 (got %d, want %d)", len(stmt.Values), len(insertCols))
	}

	// 转换值
	// 填充行数据（处理用户值或默认值）
	for i, tableCol := range table.Columns {
		// 优先使用用户提供的值，否则使用默认值
		var expr ast.Expression
		expr = userColMap[tableCol.Name]
		if expr == nil && tableCol.Default != nil {
			expr = tableCol.Default.(*ast.DefaultExpression).Value
		}
		//获取当前列名
		colName := table.Columns[i].Name
		tableColIdx, ok := colIndexMap[colName]
		if !ok {
			return fmt.Errorf("Unknown column '%s' in INSERT statement", colName)
		}
		// 转换值类型
		value, err := evaluateExpression(expr)
		if err != nil {
			return fmt.Errorf("invalid value for column '%s': %v", colName, err)
		}

		// 类型转换（保持原有逻辑）
		switch v := value.(type) {
		case string:
			if tableCol.Type == "INT" {
				intVal, err := strconv.ParseInt(v, 10, 32)
				if err != nil {
					return fmt.Errorf("Incorrect integer value: '%s' for column '%s'", v, tableCol.Name)
				}
				row[tableColIdx] = Cell{Type: CellTypeInt, IntValue: int32(intVal)}
			} else {
				row[tableColIdx] = Cell{Type: CellTypeText, TextValue: v}
			}
		case int32:
			row[tableColIdx] = Cell{Type: CellTypeInt, IntValue: v}
		case float32:
			row[tableColIdx] = Cell{Type: CellTypeFloat, FloatValue: v}
		case time.Time:
			row[tableColIdx] = Cell{Type: CellTypeDateTime, TimeValue: v.Format("2006-01-02 15:04:05")}
		default:
			return fmt.Errorf("Unsupported value type: %T for column '%s'", value, tableCol.Name)
		}
	}

	// 检查主键约束
	for i, col := range table.Columns {
		if col.Primary {
			key := row[i].String()
			if _, exists := table.Indexes[col.Name].Values[key]; exists {
				return fmt.Errorf("Duplicate entry '%s' for key '%s'", key, col.Name)
			}
		}
	}

	// 插入数据
	rowIndex := len(table.Rows)
	table.Rows = append(table.Rows, row)

	// 更新索引
	for i, col := range table.Columns {
		if col.Primary {
			key := row[i].String()
			table.Indexes[col.Name].Values[key] = append(table.Indexes[col.Name].Values[key], rowIndex)
		}
	}

	return nil
}

// Select 查询数据
// 支持 SELECT * 、指定列和简单聚合函数
// 处理 WHERE 条件
// 返回查询结果
func (b *MemoryBackend) Select(stmt *ast.SelectStatement) (*Results, error) {
	table, exists := b.tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("Table '%s' doesn't exist", stmt.TableName)
	}

	results := &Results{
		Columns: make([]ResultColumn, 0),
		Rows:    make([][]Cell, 0),
	}

	// 检查是否为聚合函数查询
	isAggregation := false
	var aggregateFunc *ast.FunctionCall

	// 处理select列表
	if len(stmt.Fields) == 1 {
		// 检查是否为 SELECT *
		if _, ok := stmt.Fields[0].(*ast.StarExpression); ok {
			// SELECT *
			for _, col := range table.Columns {
				results.Columns = append(results.Columns, ResultColumn{
					Name: col.Name,
					Type: col.Type,
				})
			}
		} else if fn, ok := stmt.Fields[0].(*ast.FunctionCall); ok {
			// 处理函数调用
			isAggregation = true
			aggregateFunc = fn
			results.Columns = append(results.Columns, ResultColumn{
				Name: fn.Name,
				Type: "FUNCTION",
			})
		} else {
			// 处理单个标识符
			if identifier, ok := stmt.Fields[0].(*ast.Identifier); ok {
				found := false
				for _, col := range table.Columns {
					if col.Name == identifier.Value {
						results.Columns = append(results.Columns, ResultColumn{
							Name: col.Name,
							Type: col.Type,
						})
						found = true
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("Unknown column '%s' in 'field list'", identifier.Value)
				}
			} else {
				return nil, fmt.Errorf("Unsupported select expression type")
			}
		}
	} else {
		// 处理多个列
		for _, expr := range stmt.Fields {
			switch e := expr.(type) {
			case *ast.Identifier:
				// 查找列
				found := false
				for _, col := range table.Columns {
					if col.Name == e.Value {
						results.Columns = append(results.Columns, ResultColumn{
							Name: col.Name,
							Type: col.Type,
						})
						found = true
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("Unknown column '%s' in 'field list'", e.Value)
				}
			case *ast.FunctionCall:
				// 处理函数调用（多列中的函数）
				results.Columns = append(results.Columns, ResultColumn{
					Name: e.Name,
					Type: "FUNCTION",
				})
			default:
				if _, ok := e.(*ast.StarExpression); ok {
					// SELECT *
					for _, col := range table.Columns {
						results.Columns = append(results.Columns, ResultColumn{
							Name: col.Name,
							Type: col.Type,
						})
					}
				} else {
					return nil, fmt.Errorf("Unsupported select expression type")
				}
			}
		}
	}

	// 如果是聚合函数查询，直接计算结果
	if isAggregation {
		// 处理WHERE子句
		filteredRows := make([][]Cell, 0)
		for _, row := range table.Rows {
			if stmt.Where != nil {
				match, err := evaluateWhereCondition(stmt.Where, row, table.Columns)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			filteredRows = append(filteredRows, row)
		}

		functionResult := calculateFunctionResults(aggregateFunc, table, filteredRows)
		results.Rows = [][]Cell{functionResult}
		return results, nil
	}

	// 处理WHERE子句
	filteredRows := make([][]Cell, 0)
	for _, row := range table.Rows {
		if stmt.Where != nil {
			match, err := evaluateWhereCondition(stmt.Where, row, table.Columns)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, row)
	}

	// 构建结果行
	for _, row := range filteredRows {
		resultRow := make([]Cell, len(results.Columns))
		for j, col := range results.Columns {
			// 查找列在原始行中的位置
			for k, tableCol := range table.Columns {
				if tableCol.Name == col.Name {
					resultRow[j] = row[k]
					break
				}
			}
		}
		results.Rows = append(results.Rows, resultRow)
	}

	return results, nil
}

// calculateFunctionResults 计算函数结果
func calculateFunctionResults(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
	// 根据函数类型计算结果
	switch strings.ToUpper(fn.Name) {
	case "COUNT":
		return calculateCount(fn, table, rows)
	case "SUM":
		return calculateSum(fn, table, rows)
	case "AVG":
		return calculateAvg(fn, table, rows)
	case "MAX":
		return calculateMax(fn, table, rows)
	case "MIN":
		return calculateMin(fn, table, rows)
	default:
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Unknown function '%s'", fn.Name)}}
	}
}

// calculateCount 计算COUNT函数结果
func calculateCount(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
	return []Cell{{Type: CellTypeInt, IntValue: int32(len(rows))}}
}

// calculateSum 计算SUM函数结果
func calculateSum(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
	// 处理 SUM(column) 情况
	if len(fn.Params) != 1 {
		return []Cell{{Type: CellTypeText, TextValue: "ERROR: SUM function requires exactly one parameter"}}
	}
	var columnName string
	// 检查参数类型
	switch param := fn.Params[0].(type) {
	case *ast.Identifier:
		columnName = param.Value
	default:
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: SUM function requires a column name, got %T", param)}}
	}

	// 查找列索引
	colIndex := -1
	for i, col := range table.Columns {
		if col.Name == columnName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Unknown column '%s'", columnName)}}
	}

	// 计算SUM值
	var sumInt int32 = 0
	var sumFloat float32 = 0.0
	hasFloat := false

	for _, row := range rows {
		cell := row[colIndex]
		switch cell.Type {
		case CellTypeInt:
			sumInt += cell.IntValue
		case CellTypeFloat:
			// 如果之前有整数，需要转换为浮点数
			if !hasFloat {
				sumFloat = float32(sumInt)
				hasFloat = true
			}
			sumFloat += cell.FloatValue
		}
	}

	// 返回结果
	if hasFloat {
		return []Cell{{Type: CellTypeFloat, FloatValue: sumFloat}}
	}
	return []Cell{{Type: CellTypeInt, IntValue: sumInt}}
}

// calculateAvg 计算AVG函数结果
func calculateAvg(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
	// 处理 AVG(column) 情况
	if len(fn.Params) != 1 {
		return []Cell{{Type: CellTypeText, TextValue: "ERROR: AVG function requires exactly one parameter"}}
	}
	var columnName string
	// 检查参数类型
	switch param := fn.Params[0].(type) {
	case *ast.Identifier:
		columnName = param.Value
	default:
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: AVG function requires a column name, got %T", param)}}
	}

	// 查找列索引
	colIndex := -1
	for i, col := range table.Columns {
		if col.Name == columnName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Unknown column '%s'", columnName)}}
	}

	// 计算平均值
	var sumFloat float32 = 0.0
	count := 0

	for _, row := range rows {
		cell := row[colIndex]
		switch cell.Type {
		case CellTypeInt:
			sumFloat += float32(cell.IntValue)
			count++
		case CellTypeFloat:
			sumFloat += cell.FloatValue
			count++
		default:
			return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Cannot calculate AVG for non-numeric column '%s'", columnName)}}
		}
	}

	// 如果没有行，返回 NULL 或 0
	if count == 0 {
		return []Cell{{Type: CellTypeInt, IntValue: 0}}
	}

	avg := sumFloat / float32(count)
	return []Cell{{Type: CellTypeFloat, FloatValue: avg}}
}

// calculateMax 计算MAX函数结果
func calculateMax(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
	// 处理 MAX(column) 情况
	if len(fn.Params) != 1 {
		return []Cell{{Type: CellTypeText, TextValue: "ERROR: MAX function requires exactly one parameter"}}
	}
	var columnName string
	// 检查参数类型
	switch param := fn.Params[0].(type) {
	case *ast.Identifier:
		columnName = param.Value
	default:
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: MAX function requires a column name, got %T", param)}}
	}

	// 查找列索引
	colIndex := -1
	for i, col := range table.Columns {
		if col.Name == columnName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Unknown column '%s'", columnName)}}
	}

	// 确定列的数据类型
	var columnType CellType
	if len(rows) > 0 {
		columnType = rows[0][colIndex].Type
	} else {
		// 如果没有数据行，返回默认值
		return []Cell{{Type: CellTypeInt, IntValue: 0}}
	}

	// 计算最大值
	switch columnType {
	case CellTypeInt:
		maxVal := rows[0][colIndex].IntValue
		for _, row := range rows {
			cell := row[colIndex]
			if cell.Type == CellTypeInt && cell.IntValue > maxVal {
				maxVal = cell.IntValue
			}
		}
		return []Cell{{Type: CellTypeInt, IntValue: maxVal}}
	case CellTypeFloat:
		maxVal := rows[0][colIndex].FloatValue
		for _, row := range rows {
			cell := row[colIndex]
			switch cell.Type {
			case CellTypeFloat:
				if cell.FloatValue > maxVal {
					maxVal = cell.FloatValue
				}
			case CellTypeInt:
				if float32(cell.IntValue) > maxVal {
					maxVal = float32(cell.IntValue)
				}
			}
		}
		return []Cell{{Type: CellTypeFloat, FloatValue: maxVal}}
	case CellTypeText:
		maxVal := rows[0][colIndex].TextValue
		for _, row := range rows {
			cell := row[colIndex]
			if cell.Type == CellTypeText && cell.TextValue > maxVal {
				maxVal = cell.TextValue
			}
		}
		return []Cell{{Type: CellTypeText, TextValue: maxVal}}
	default:
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Unsupported column type for MAX function")}}
	}
}

// calculateMin 计算MIN函数结果
func calculateMin(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
	// 处理 MIN(column) 情况
	if len(fn.Params) != 1 {
		return []Cell{{Type: CellTypeText, TextValue: "ERROR: MIN function requires exactly one parameter"}}
	}
	var columnName string
	// 检查参数类型
	switch param := fn.Params[0].(type) {
	case *ast.Identifier:
		columnName = param.Value
	default:
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: MIN function requires a column name, got %T", param)}}
	}

	// 查找列索引
	colIndex := -1
	for i, col := range table.Columns {
		if col.Name == columnName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Unknown column '%s'", columnName)}}
	}

	// 如果没有数据行，返回默认值
	if len(rows) == 0 {
		return []Cell{{Type: CellTypeInt, IntValue: 0}}
	}

	// 确定列的数据类型
	var columnType CellType
	columnType = rows[0][colIndex].Type

	// 计算最小值
	switch columnType {
	case CellTypeInt:
		minVal := rows[0][colIndex].IntValue
		for _, row := range rows {
			cell := row[colIndex]
			if cell.Type == CellTypeInt && cell.IntValue < minVal {
				minVal = cell.IntValue
			}
		}
		return []Cell{{Type: CellTypeInt, IntValue: minVal}}
	case CellTypeFloat:
		minVal := rows[0][colIndex].FloatValue
		for _, row := range rows {
			cell := row[colIndex]
			switch cell.Type {
			case CellTypeFloat:
				if cell.FloatValue < minVal {
					minVal = cell.FloatValue
				}
			case CellTypeInt:
				if float32(cell.IntValue) < minVal {
					minVal = float32(cell.IntValue)
				}
			}
		}
		return []Cell{{Type: CellTypeFloat, FloatValue: minVal}}
	case CellTypeText:
		minVal := rows[0][colIndex].TextValue
		for _, row := range rows {
			cell := row[colIndex]
			if cell.Type == CellTypeText && cell.TextValue < minVal {
				minVal = cell.TextValue
			}
		}
		return []Cell{{Type: CellTypeText, TextValue: minVal}}
	default:
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Unsupported column type for MIN function")}}
	}
}

// Update 执行UPDATE操作
// 验证表和列存在性
// 处理 WHERE 条件
// 更新符合条件的行
func (mb *MemoryBackend) Update(stmt *ast.UpdateStatement) error {
	table, ok := mb.tables[stmt.TableName]
	if !ok {
		return fmt.Errorf("Table '%s' doesn't exist", stmt.TableName)
	}

	// 获取列索引
	columnIndices := make(map[string]int)
	for i, col := range table.Columns {
		columnIndices[col.Name] = i
	}

	// 验证所有要更新的列是否存在
	for _, set := range stmt.Set {
		if _, ok := columnIndices[set.Column]; !ok {
			return fmt.Errorf("Unknown column '%s' in 'field list'", set.Column)
		}
	}

	// 更新符合条件的行
	for i := range table.Rows {
		if stmt.Where != nil {
			// 评估WHERE条件
			result, err := evaluateWhereCondition(stmt.Where, table.Rows[i], table.Columns)
			if err != nil {
				return err
			}
			if !result {
				continue
			}
		}

		// 更新行
		for _, set := range stmt.Set {
			colIndex := columnIndices[set.Column]
			value, err := evaluateExpression(set.Value)
			if err != nil {
				return err
			}

			switch v := value.(type) {
			case int32:
				table.Rows[i][colIndex] = Cell{Type: CellTypeInt, IntValue: v}
			case string:
				table.Rows[i][colIndex] = Cell{Type: CellTypeText, TextValue: v}
			case float32:
				table.Rows[i][colIndex] = Cell{Type: CellTypeFloat, FloatValue: v}
			case time.Time:
				table.Rows[i][colIndex] = Cell{Type: CellTypeDateTime, TimeValue: v.String()}
			default:
				return fmt.Errorf("Unsupported value type: %T for column '%s'", value, set.Column)
			}
		}
	}

	return nil
}

// Delete 执行DELETE操作
// 验证表存在性
// 处理 WHERE 条件
// 删除符合条件的行
func (mb *MemoryBackend) Delete(stmt *ast.DeleteStatement) error {
	table, ok := mb.tables[stmt.TableName]
	if !ok {
		return fmt.Errorf("Table '%s' doesn't exist", stmt.TableName)
	}

	// 找出要删除的行
	rowsToDelete := make([]int, 0)
	for i := range table.Rows {
		if stmt.Where != nil {
			// 评估WHERE条件
			result, err := evaluateWhereCondition(stmt.Where, table.Rows[i], table.Columns)
			if err != nil {
				return err
			}
			if !result {
				continue
			}
		}
		rowsToDelete = append(rowsToDelete, i)
	}

	// 从后向前删除行，以避免索引变化
	for i := len(rowsToDelete) - 1; i >= 0; i-- {
		rowIndex := rowsToDelete[i]
		table.Rows = append(table.Rows[:rowIndex], table.Rows[rowIndex+1:]...)
	}

	return nil
}

// DropTable 删除表
// 验证表是否存在
// 从存储引擎中删除表
func (mb *MemoryBackend) DropTable(stmt *ast.DropTableStatement) error {
	if _, exists := mb.tables[stmt.TableName]; !exists {
		return fmt.Errorf("Unknown table '%s'", stmt.TableName)
	}

	delete(mb.tables, stmt.TableName)
	return nil
}

// evaluateExpression 评估表达式的值
// 计算表达式的值
// 处理不同类型的数据
func evaluateExpression(expr ast.Expression) (interface{}, error) {
	switch e := expr.(type) {
	case *ast.IntegerLiteral:
		val, err := strconv.ParseInt(e.Value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Incorrect integer value: '%s'", e.Value)
		}
		return int32(val), nil
	case *ast.FloatLiteral:
		val, err := strconv.ParseFloat(e.Value, 32)
		if err != nil {
			return nil, fmt.Errorf("Incorrect float value: '%s'", e.Value)
		}
		return float32(val), nil
	case *ast.DateTimeLiteral:
		t, err := time.Parse("2006-01-02 15:04:05", e.Value)
		if err != nil {
			return nil, fmt.Errorf("Incorrect datetime value: '%s'", e.Value)
		}
		return t, nil
	case *ast.StringLiteral:
		return e.Value, nil
	case *ast.Identifier:
		return nil, fmt.Errorf("Cannot evaluate identifier: '%s'", e.Value)
	case *ast.FunctionCall:
		// 对于聚合函数，我们应该返回一个特殊的错误，指示它应该在Select方法中处理
		// 这样可以避免在错误的路径中处理函数调用
		functionName := strings.ToUpper(e.Name)
		if functionName == "COUNT" || functionName == "SUM" || functionName == "AVG" || functionName == "MAX" || functionName == "MIN" {
			return nil, fmt.Errorf("Aggregate functions should be handled in Select method")
		}
		return evaluateFunctionCall(e)
	default:
		return nil, fmt.Errorf("Unknown expression type: %T", expr)
	}
}

// evaluateFunctionCall 评估函数调用（只处理非聚合函数）
func evaluateFunctionCall(fn *ast.FunctionCall) (interface{}, error) {
	// 这里可以实现非聚合函数的处理逻辑
	return nil, fmt.Errorf("Unsupported function: %s", fn.Name)
}

// matchLikePattern 检查字符串是否匹配LIKE模式
func matchLikePattern(str, pattern string) bool {
	// 将SQL LIKE模式转换为正则表达式
	regexPattern := "^"
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			regexPattern += ".*"
		case '_':
			regexPattern += "."
		case '\\':
			if i+1 < len(pattern) {
				regexPattern += "\\" + string(pattern[i+1])
				i++
			}
		default:
			// 转义正则表达式特殊字符
			if strings.ContainsAny(string(pattern[i]), ".+*?^$()[]{}|") {
				regexPattern += "\\" + string(pattern[i])
			} else {
				regexPattern += string(pattern[i])
			}
		}
	}
	regexPattern += "$"

	// 编译正则表达式
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return false
	}

	// 执行匹配
	return re.MatchString(str)
}

// evaluateWhereCondition 评估WHERE条件
// 评估 WHERE 条件
// 支持比较运算符和 LIKE 操作符
func evaluateWhereCondition(expr ast.Expression, row []Cell, columns []ast.ColumnDefinition) (bool, error) {
	switch e := expr.(type) {
	case *ast.BinaryExpression:
		// 获取左操作数的值
		leftValue, err := getColumnValue(e.Left, row, columns)
		if err != nil {
			return false, err
		}

		// 获取右操作数的值
		rightValue, err := getColumnValue(e.Right, row, columns)
		if err != nil {
			return false, err
		}

		// 根据操作符比较值
		switch e.Operator {
		case "=":
			return compareValues(leftValue, rightValue, "=")
		case ">":
			return compareValues(leftValue, rightValue, ">")
		case "<":
			return compareValues(leftValue, rightValue, "<")
		case ">=":
			return compareValues(leftValue, rightValue, ">=")
		case "<=":
			return compareValues(leftValue, rightValue, "<=")
		case "!=":
			result, err := compareValues(leftValue, rightValue, "=")
			if err != nil {
				return false, err
			}
			return !result, nil // 返回相反的结果
		default:
			return false, fmt.Errorf("Unknown operator: '%s'", e.Operator)
		}
	case *ast.LikeExpression:
		// 获取左操作数的值
		leftValue, err := getColumnValue(e.Left, row, columns)
		if err != nil {
			return false, err
		}

		// 确保左操作数是字符串类型
		strValue, ok := leftValue.(string)
		if !ok {
			return false, fmt.Errorf("LIKE operator requires string operand")
		}

		// 执行LIKE匹配
		return matchLikePattern(strValue, e.Pattern), nil
	case *ast.BetweenExpression:
		// 解析需要比较的字段（between左侧的字段）
		colIndex, err := getColumnIndex(e.Left.(*ast.Identifier).Value, columns)
		if err != nil {
			return false, err
		}

		// 获取列值
		left := row[colIndex]
		lower, err := evaluateExpression(e.Low)
		if err != nil {
			return false, err
		}

		upper, err := evaluateExpression(e.High)
		if err != nil {
			return false, err
		}

		switch left.Type {
		case CellTypeInt:
			leftVal := left.IntValue
			lowerVal, lok := lower.(int32)
			upperVal, uok := upper.(int32)
			if !lok || !uok {
				return false, fmt.Errorf("type mismatch in BETWEEN expression")
			}
			return leftVal >= lowerVal && leftVal <= upperVal, nil
		case CellTypeFloat:
			leftVal := left.FloatValue
			lowerVal, lok := lower.(float32)
			upperVal, uok := upper.(float32)
			if !lok || !uok {
				return false, fmt.Errorf("type mismatch in BETWEEN expression")
			}
			return leftVal >= lowerVal && leftVal <= upperVal, nil
		case CellTypeDateTime:
			val := left.TimeValue
			leftVal, err := time.Parse("2006-01-02 15:04:05", val)
			if err != nil {
				return false, err
			}
			lowerVal, lok := lower.(time.Time)
			upperVal, uok := upper.(time.Time)
			if !lok || !uok {
				return false, fmt.Errorf("type mismatch in BETWEEN expression")
			}
			return (leftVal.After(lowerVal) || leftVal.Equal(lowerVal)) &&
				(leftVal.Before(upperVal) || leftVal.Equal(upperVal)), nil
		default:
			return false, fmt.Errorf("unsupported type in BETWEEN expression")
		}
	default:
		return false, fmt.Errorf("Unknown expression type: %T", expr)
	}
}

// compareValues 比较两个值
func compareValues(left, right interface{}, operator string) (bool, error) {
	// 首先检查类型是否匹配
	if reflect.TypeOf(left) != reflect.TypeOf(right) {
		// 特殊处理数字类型间的比较
		if isNumericType(left) && isNumericType(right) {
			return compareNumericValues(left, right, operator)
		}
		return false, fmt.Errorf("Cannot compare values of different types: %T and %T", left, right)
	}

	switch operator {
	case "=":
		return isEqual(left, right)
	case ">":
		return isGreater(left, right)
	case "<":
		return isLess(left, right)
	case ">=":
		equal, _ := isEqual(left, right)
		greater, _ := isGreater(left, right)
		return equal || greater, nil
	case "<=":
		equal, _ := isEqual(left, right)
		less, _ := isLess(left, right)
		return equal || less, nil
	case "!=":
		equal, err := isEqual(left, right)
		if err != nil {
			return false, err
		}
		return !equal, nil
	default:
		return false, fmt.Errorf("Unknown operator: '%s'", operator)
	}
}

// 辅助函数：检查是否为数字类型
func isNumericType(v interface{}) bool {
	switch v.(type) {
	case int32, float32:
		return true
	default:
		return false
	}
}

// 辅助函数：比较数字类型值
func compareNumericValues(left, right interface{}, operator string) (bool, error) {
	// 转换为 float32 进行比较
	var leftVal, rightVal float32

	switch l := left.(type) {
	case int32:
		leftVal = float32(l)
	case float32:
		leftVal = l
	}

	switch r := right.(type) {
	case int32:
		rightVal = float32(r)
	case float32:
		rightVal = r
	}

	switch operator {
	case "=":
		return leftVal == rightVal, nil
	case ">":
		return leftVal > rightVal, nil
	case "<":
		return leftVal < rightVal, nil
	case ">=":
		return leftVal >= rightVal, nil
	case "<=":
		return leftVal <= rightVal, nil
	case "!=":
		return leftVal != rightVal, nil
	default:
		return false, fmt.Errorf("Unknown operator: '%s'", operator)
	}
}

// 辅助函数：判断是否相等
func isEqual(left, right interface{}) (bool, error) {
	switch l := left.(type) {
	case int32:
		if r, ok := right.(int32); ok {
			return l == r, nil
		}
	case string:
		if r, ok := right.(string); ok {
			return l == r, nil
		}
	case float32:
		if r, ok := right.(float32); ok {
			return l == r, nil
		}
	case time.Time:
		if r, ok := right.(time.Time); ok {
			return l.Equal(r), nil
		}
	}
	return false, fmt.Errorf("Cannot compare values of different types: %T and %T", left, right)
}

// 辅助函数：判断是否大于
func isGreater(left, right interface{}) (bool, error) {
	switch l := left.(type) {
	case int32:
		if r, ok := right.(int32); ok {
			return l > r, nil
		}
	case string:
		if r, ok := right.(string); ok {
			return l > r, nil
		}
	case float32:
		if r, ok := right.(float32); ok {
			return l > r, nil
		}
	case time.Time:
		if r, ok := right.(time.Time); ok {
			return l.After(r), nil
		}
	}
	return false, fmt.Errorf("Cannot compare values of different types: %T and %T", left, right)
}

// 辅助函数：判断是否小于
func isLess(left, right interface{}) (bool, error) {
	switch l := left.(type) {
	case int32:
		if r, ok := right.(int32); ok {
			return l < r, nil
		}
	case string:
		if r, ok := right.(string); ok {
			return l < r, nil
		}
	case float32:
		if r, ok := right.(float32); ok {
			return l < r, nil
		}
	case time.Time:
		if r, ok := right.(time.Time); ok {
			return l.Before(r), nil
		}
	}
	return false, fmt.Errorf("Cannot compare values of different types: %T and %T", left, right)
}

// getColumnValue 获取列的值
func getColumnValue(expr ast.Expression, row []Cell, columns []ast.ColumnDefinition) (interface{}, error) {
	switch e := expr.(type) {
	case *ast.Identifier:
		// 查找列索引
		for i, col := range columns {
			if col.Name == e.Value {
				switch row[i].Type {
				case CellTypeInt:
					return row[i].IntValue, nil
				case CellTypeText:
					return row[i].TextValue, nil
				case CellTypeFloat:
					return row[i].FloatValue, nil
				case CellTypeDateTime:
					str := row[i].TimeValue
					val, err := time.Parse("2006-01-02 15:04:05", str)
					if err != nil {
						return nil, err
					}
					return val, nil
				default:
					return nil, fmt.Errorf("Unknown cell type: %v", row[i].Type)
				}
			}
		}
		return nil, fmt.Errorf("Unknown column '%s' in 'where clause'", e.Value)
	case *ast.IntegerLiteral:
		val, err := strconv.ParseInt(e.Value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Incorrect integer value: '%s'", e.Value)
		}
		return int32(val), nil
	case *ast.StringLiteral:
		return e.Value, nil
	case *ast.FloatLiteral:
		val, err := strconv.ParseFloat(e.Value, 32)
		if err != nil {
			return nil, fmt.Errorf("Incorrect float value: '%s'", e.Value)
		}
		return float32(val), nil
	case *ast.DateTimeLiteral:
		val, err := time.Parse("2006-01-02 15:04:05", e.Value)
		if err != nil {
			return nil, fmt.Errorf("Incorrect datetime value: '%s'", e.Value)
		}
		return val, nil
	default:
		return nil, fmt.Errorf("Unknown expression type: %T", expr)
	}
}

// getColumnIndex 根据列名获取列索引
func getColumnIndex(columnName string, columns []ast.ColumnDefinition) (int, error) {
	for i, col := range columns {
		if col.Name == columnName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column '%s' not found", columnName)
}

//后续拓展新的存储引擎，如落地到文件...
