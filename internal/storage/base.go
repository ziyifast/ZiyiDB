// internal/storage/base.go
package storage

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"ziyi.db.com/internal/ast"
)

// BaseEngine 提供通用的业务逻辑实现
type BaseEngine struct{}

// Insert 通用插入逻辑
func (b *BaseEngine) Insert(table *Table, stmt *ast.InsertStatement, txn *Transaction,
	getVisibleRow func([]VersionedCell, *Transaction) []Cell,
	convertToCell func(interface{}, string) Cell,
	evaluateExpression func(ast.Expression) (interface{}, error)) error {

	// 初始化行数据（长度为表的总列数）
	row := make([]Cell, len(table.Columns))

	// 处理插入列列表（用户显式指定的列或隐式全列）
	if len(stmt.Columns) > 0 {
		// 用户指定了列名
		if len(stmt.Columns) != len(stmt.Values) {
			return fmt.Errorf("Column count doesn't match value count at row 1 (got %d, want %d)", len(stmt.Values), len(stmt.Columns))
		}

		// 构建列名到表列索引的映射
		colIndexMap := make(map[string]int)
		for idx, col := range table.Columns {
			colIndexMap[col.Name] = idx
		}

		// 填充指定的列
		for i, col := range stmt.Columns {
			colIndex, exists := colIndexMap[col.Value]
			if !exists {
				return fmt.Errorf("Unknown column '%s' in INSERT statement", col.Value)
			}

			value, err := evaluateExpression(stmt.Values[i])
			if err != nil {
				return fmt.Errorf("invalid value for column '%s': %v", col.Value, err)
			}

			// 类型转换
			row[colIndex] = convertToCell(value, table.Columns[colIndex].Type)
		}
	} else {
		// 用户未指定列名，使用所有列
		if len(stmt.Values) != len(table.Columns) {
			return fmt.Errorf("Column count doesn't match value count at row 1 (got %d, want %d)", len(stmt.Values), len(table.Columns))
		}

		// 填充所有列
		for i, expr := range stmt.Values {
			value, err := evaluateExpression(expr)
			if err != nil {
				return fmt.Errorf("invalid value for column '%s': %v", table.Columns[i].Name, err)
			}

			// 类型转换
			row[i] = convertToCell(value, table.Columns[i].Type)
		}
	}

	// 处理默认值（对于未指定的列）
	for i, col := range table.Columns {
		// 如果该列没有被赋值且有默认值
		if row[i].Type == 0 && col.Default != nil {
			defaultExpr := col.Default.(*ast.DefaultExpression)
			value, err := evaluateExpression(defaultExpr.Value)
			if err != nil {
				return fmt.Errorf("invalid default value for column '%s': %v", col.Name, err)
			}

			// 类型转换
			row[i] = convertToCell(value, col.Type)
		}
	}

	// 检查主键约束
	for i, col := range table.Columns {
		if col.Primary {
			key := row[i].String()
			// 直接使用索引检查冲突
			if rowIndexes, exists := table.Indexes[col.Name].Values[key]; exists {
				// 检查这些索引指向的行是否与当前插入的行冲突
				for _, rowIndex := range rowIndexes {
					if rowIndex < len(table.Rows) {
						versionedRow := table.Rows[rowIndex]
						// 在事务上下文中检查是否存在可见的冲突行
						visibleRow := getVisibleRow(versionedRow, txn)
						if visibleRow != nil && visibleRow[i].String() == key {
							// 存在具有相同主键的可见行，违反主键约束
							return fmt.Errorf("Duplicate entry '%s' for key '%s'", key, col.Name)
						}
					}
				}
			}
		}
	}

	// 创建版本化单元格
	versionedCells := make([]VersionedCell, len(table.Columns))
	txnID := uint64(0)
	if txn != nil {
		txnID = txn.ID
	}

	for i, cell := range row {
		versionedCells[i] = VersionedCell{
			Data:      cell,
			TxnID:     txnID,
			Timestamp: time.Now(),
			Committed: txn == nil, // 如果没有事务，则立即提交（自动提交模式）
		}
	}

	// 插入数据
	rowIndex := len(table.Rows)
	table.Rows = append(table.Rows, versionedCells)

	// 更新索引
	for i, col := range table.Columns {
		if col.Primary {
			key := row[i].String()
			table.Indexes[col.Name].Values[key] = append(table.Indexes[col.Name].Values[key], rowIndex)
		}
	}

	// 记录写入的行
	if txn != nil {
		txn.AddToWriteSet(stmt.TableName, rowIndex)
	}

	return nil
}

// Select 通用查询逻辑
func (b *BaseEngine) Select(table *Table, stmt *ast.SelectStatement, txn *Transaction,
	getVisibleRow func([]VersionedCell, *Transaction) []Cell,
	evaluateWhereCondition func(ast.Expression, []Cell, []ast.ColumnDefinition) (bool, error)) (*Results, error) {

	results := &Results{
		Columns: make([]ResultColumn, 0),
		Rows:    make([][]Cell, 0),
	}

	// 如果有 GROUP BY 子句
	if len(stmt.GroupBy) > 0 {
		res, err := b.selectWithGroupBy(table, stmt, txn, getVisibleRow, evaluateWhereCondition)
		if err != nil {
			return nil, err
		}

		// 处理 ORDER BY（在 GROUP BY 之后）
		if len(stmt.OrderBy) > 0 {
			res.Rows, err = b.orderBy(res.Rows, res.Columns, stmt.OrderBy, table.Columns)
			if err != nil {
				return nil, err
			}
		}

		return res, nil
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
		for i, row := range table.Rows {
			// 在事务上下文中读取最新可见版本
			visibleRow := getVisibleRow(row, txn)
			if visibleRow == nil {
				continue
			}

			if stmt.Where != nil {
				match, err := evaluateWhereCondition(stmt.Where, visibleRow, table.Columns)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			filteredRows = append(filteredRows, visibleRow)

			// 记录读取的行
			if txn != nil {
				txn.AddToReadSet(stmt.TableName, i)
			}
		}

		functionResult := b.calculateFunctionResults(aggregateFunc, table, filteredRows)
		results.Rows = [][]Cell{functionResult}

		// 聚合函数结果通常只有一行，不需要排序
		return results, nil
	}

	// 处理WHERE子句
	filteredRows := make([][]Cell, 0)
	for i, row := range table.Rows {
		// 在事务上下文中读取最新可见版本
		visibleRow := getVisibleRow(row, txn)
		if visibleRow == nil {
			continue
		}

		if stmt.Where != nil {
			match, err := evaluateWhereCondition(stmt.Where, visibleRow, table.Columns)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, visibleRow)

		// 记录读取的行
		if txn != nil {
			txn.AddToReadSet(stmt.TableName, i)
		}
	}

	// 构建结果行
	for _, row := range filteredRows {
		resultRow := make([]Cell, len(results.Columns))
		for j, col := range results.Columns {
			// 查找列在原始行中的位置
			found := false
			for k, tableCol := range table.Columns {
				if tableCol.Name == col.Name {
					// 确保索引在有效范围内
					if k < len(row) {
						resultRow[j] = row[k]
						found = true
						break
					}
				}
			}
			// 如果没找到对应的列，设置为默认值
			if !found {
				resultRow[j] = Cell{Type: CellTypeText, TextValue: "NULL"}
			}
		}
		results.Rows = append(results.Rows, resultRow)
	}

	// 处理 ORDER BY
	if len(stmt.OrderBy) > 0 {
		var err error
		results.Rows, err = b.orderBy(results.Rows, results.Columns, stmt.OrderBy, table.Columns)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// Update 通用更新逻辑
func (b *BaseEngine) Update(table *Table, stmt *ast.UpdateStatement, txn *Transaction,
	getVisibleRow func([]VersionedCell, *Transaction) []Cell,
	convertToCell func(interface{}, string) Cell,
	evaluateExpression func(ast.Expression) (interface{}, error),
	evaluateWhereCondition func(ast.Expression, []Cell, []ast.ColumnDefinition) (bool, error)) error {

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
		// 获取可见行数据
		visibleRow := getVisibleRow(table.Rows[i], txn)
		if visibleRow == nil {
			continue
		}

		if stmt.Where != nil {
			// 评估WHERE条件
			result, err := evaluateWhereCondition(stmt.Where, visibleRow, table.Columns)
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

			txnID := uint64(0)
			if txn != nil {
				txnID = txn.ID
			}

			switch v := value.(type) {
			case int32:
				table.Rows[i][colIndex] = VersionedCell{
					Data:      Cell{Type: CellTypeInt, IntValue: v},
					TxnID:     txnID,
					Timestamp: time.Now(),
					Committed: txn == nil, // 如果没有事务，则立即提交
				}
			case string:
				table.Rows[i][colIndex] = VersionedCell{
					Data:      Cell{Type: CellTypeText, TextValue: v},
					TxnID:     txnID,
					Timestamp: time.Now(),
					Committed: txn == nil, // 如果没有事务，则立即提交
				}
			case float32:
				table.Rows[i][colIndex] = VersionedCell{
					Data:      Cell{Type: CellTypeFloat, FloatValue: v},
					TxnID:     txnID,
					Timestamp: time.Now(),
					Committed: txn == nil, // 如果没有事务，则立即提交
				}
			case time.Time:
				table.Rows[i][colIndex] = VersionedCell{
					Data:      Cell{Type: CellTypeDateTime, TimeValue: v.Format("2006-01-02 15:04:05")},
					TxnID:     txnID,
					Timestamp: time.Now(),
					Committed: txn == nil, // 如果没有事务，则立即提交
				}
			default:
				return fmt.Errorf("Unsupported value type: %T for column '%s'", value, set.Column)
			}
		}

		// 记录写入的行
		if txn != nil {
			txn.AddToWriteSet(stmt.TableName, i)
		}
	}

	return nil
}

// Delete 通用删除逻辑
func (b *BaseEngine) Delete(table *Table, stmt *ast.DeleteStatement, txn *Transaction,
	getVisibleRow func([]VersionedCell, *Transaction) []Cell,
	evaluateWhereCondition func(ast.Expression, []Cell, []ast.ColumnDefinition) (bool, error)) error {

	// 找出要删除的行
	rowsToDelete := make([]int, 0)
	for i := range table.Rows {
		// 获取可见行数据
		visibleRow := getVisibleRow(table.Rows[i], txn)
		if visibleRow == nil {
			continue
		}

		if stmt.Where != nil {
			// 评估WHERE条件
			result, err := evaluateWhereCondition(stmt.Where, visibleRow, table.Columns)
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

// selectWithGroupBy 处理带有 GROUP BY 的查询
func (b *BaseEngine) selectWithGroupBy(table *Table, stmt *ast.SelectStatement, txn *Transaction,
	getVisibleRow func([]VersionedCell, *Transaction) []Cell,
	evaluateWhereCondition func(ast.Expression, []Cell, []ast.ColumnDefinition) (bool, error)) (*Results, error) {

	results := &Results{
		Columns: make([]ResultColumn, 0),
		Rows:    make([][]Cell, 0),
	}

	// 验证 GROUP BY 字段存在于表中
	groupByIndices := make([]int, len(stmt.GroupBy))
	for i, expr := range stmt.GroupBy {
		if identifier, ok := expr.(*ast.Identifier); ok {
			found := false
			for j, col := range table.Columns {
				if col.Name == identifier.Value {
					groupByIndices[i] = j
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("Unknown column '%s' in 'group statement'", identifier.Value)
			}
		} else {
			return nil, fmt.Errorf("GROUP BY only supports column names")
		}
	}

	// 构建结果列
	for _, expr := range stmt.Fields {
		switch e := expr.(type) {
		case *ast.Identifier:
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
			results.Columns = append(results.Columns, ResultColumn{
				Name: e.Name,
				Type: "FUNCTION",
			})
		case *ast.StarExpression:
			for _, col := range table.Columns {
				results.Columns = append(results.Columns, ResultColumn{
					Name: col.Name,
					Type: col.Type,
				})
			}
		default:
			return nil, fmt.Errorf("Unsupported select expression type")
		}
	}

	// 处理WHERE子句
	filteredRows := make([][]Cell, 0)
	for i, versionedRow := range table.Rows {
		// 在事务上下文中读取最新可见版本
		visibleRow := getVisibleRow(versionedRow, txn)
		if visibleRow == nil {
			continue
		}

		if stmt.Where != nil {
			match, err := evaluateWhereCondition(stmt.Where, visibleRow, table.Columns)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, visibleRow)

		// 记录读取的行
		if txn != nil {
			txn.AddToReadSet(stmt.TableName, i)
		}
	}

	// 按 GROUP BY 字段分组
	groups := make(map[string][][]Cell)
	for _, row := range filteredRows {
		// 构建分组键
		groupKey := ""
		for _, idx := range groupByIndices {
			// 确保索引在有效范围内
			if idx < len(row) {
				groupKey += row[idx].String() + "|"
			}
		}

		// 将行添加到对应的组中
		groups[groupKey] = append(groups[groupKey], row)
	}

	// 为每个组计算结果
	for _, groupRows := range groups {
		if len(groupRows) == 0 {
			continue
		}

		resultRow := make([]Cell, len(results.Columns))
		colIndex := 0

		// 处理非聚合字段（GROUP BY 字段）
		for _, expr := range stmt.Fields {
			if identifier, ok := expr.(*ast.Identifier); ok {
				// 检查是否为 GROUP BY 字段
				isGroupByField := false
				for _, groupByExpr := range stmt.GroupBy {
					if groupByIdent, ok := groupByExpr.(*ast.Identifier); ok {
						if groupByIdent.Value == identifier.Value {
							isGroupByField = true
							break
						}
					}
				}

				if isGroupByField && len(groupRows) > 0 {
					// 对于 GROUP BY 字段，取第一个值（所有行应该相同）
					found := false
					for k, tableCol := range table.Columns {
						if tableCol.Name == identifier.Value && k < len(groupRows[0]) {
							resultRow[colIndex] = groupRows[0][k]
							found = true
							break
						}
					}
					// 如果没找到，设置为默认值
					if !found {
						resultRow[colIndex] = Cell{Type: CellTypeText, TextValue: "NULL"}
					}
				}
				colIndex++
			}
		}

		// 处理聚合函数
		for i, expr := range stmt.Fields {
			if fn, ok := expr.(*ast.FunctionCall); ok {
				functionResult := b.calculateFunctionResults(fn, table, groupRows)
				if len(functionResult) > 0 {
					resultRow[i] = functionResult[0]
				} else {
					resultRow[i] = Cell{Type: CellTypeText, TextValue: "NULL"}
				}
			}
		}

		results.Rows = append(results.Rows, resultRow)
	}

	return results, nil
}

// calculateFunctionResults 计算函数结果
func (b *BaseEngine) calculateFunctionResults(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
	// 根据函数类型计算结果
	switch strings.ToUpper(fn.Name) {
	case "COUNT":
		return b.calculateCount(fn, table, rows)
	case "SUM":
		return b.calculateSum(fn, table, rows)
	case "AVG":
		return b.calculateAvg(fn, table, rows)
	case "MAX":
		return b.calculateMax(fn, table, rows)
	case "MIN":
		return b.calculateMin(fn, table, rows)
	default:
		return []Cell{{Type: CellTypeText, TextValue: fmt.Sprintf("ERROR: Unknown function '%s'", fn.Name)}}
	}
}

// calculateCount 计算COUNT函数结果
func (b *BaseEngine) calculateCount(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
	return []Cell{{Type: CellTypeInt, IntValue: int32(len(rows))}}
}

// calculateSum 计算SUM函数结果
func (b *BaseEngine) calculateSum(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
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
func (b *BaseEngine) calculateAvg(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
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
func (b *BaseEngine) calculateMax(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
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
func (b *BaseEngine) calculateMin(fn *ast.FunctionCall, table *Table, rows [][]Cell) []Cell {
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

// orderBy 根据 ORDER BY 子句对结果进行排序
func (b *BaseEngine) orderBy(rows [][]Cell, resultCols []ResultColumn, orderBy []ast.OrderByClause, tableCols []ast.ColumnDefinition) ([][]Cell, error) {
	// 创建列名到索引的映射
	colIndexMap := make(map[string]int)
	for i, col := range resultCols {
		colIndexMap[col.Name] = i
	}

	// 创建排序键的索引和方向
	type sortKey struct {
		index     int
		direction string
	}

	var sortKeys []sortKey
	for _, ob := range orderBy {
		identifier, ok := ob.Expression.(*ast.Identifier)
		if !ok {
			return nil, fmt.Errorf("ORDER BY only supports column names")
		}

		index, exists := colIndexMap[identifier.Value]
		if !exists {
			return nil, fmt.Errorf("Unknown column '%s' in 'order clause'", identifier.Value)
		}

		sortKeys = append(sortKeys, sortKey{
			index:     index,
			direction: ob.Direction,
		})
	}

	// 使用 sort.Slice 进行排序
	sort.Slice(rows, func(i, j int) bool {
		for _, key := range sortKeys {
			left := rows[i][key.index]
			right := rows[j][key.index]

			// 比较两个值
			result, err := compareValues(left, right, "<")
			if err != nil {
				// 如果比较出错，保持原有顺序
				return false
			}

			if result {
				// 如果是升序，返回 true
				// 如果是降序，返回 false
				return key.direction == "ASC"
			} else {
				// 检查是否相等
				equal, _ := compareValues(left, right, "=")
				if !equal {
					// 如果是降序，返回 true
					// 如果是升序，返回 false
					return key.direction == "DESC"
				}
				// 如果相等，继续比较下一个排序键
			}
		}
		// 所有键都相等，保持原有顺序
		return false
	})

	return rows, nil
}

// evaluateExpression 评估表达式的值
func (b *BaseEngine) evaluateExpression(expr ast.Expression) (interface{}, error) {
	switch e := expr.(type) {
	case *ast.IntegerLiteral:
		return e.Value, nil // 直接返回已解析的值
	case *ast.FloatLiteral:
		return e.Value, nil // 直接返回已解析的值
	case *ast.DateTimeLiteral:
		return e.Value, nil // 直接返回已解析的值
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
		return nil, fmt.Errorf("Unsupported function: %s", e.Name)
	default:
		return nil, fmt.Errorf("Unknown expression type: %T", expr)
	}
}

// matchLikePattern 检查字符串是否匹配LIKE模式
func (b *BaseEngine) matchLikePattern(str, pattern string) bool {
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

// compareValues 比较两个值
func (b *BaseEngine) compareValues(left, right interface{}, operator string) (bool, error) {
	// 如果参数是 Cell 类型，提取其值
	if leftCell, ok := left.(Cell); ok {
		left = getCellValue(leftCell)
	}

	if rightCell, ok := right.(Cell); ok {
		right = getCellValue(rightCell)
	}

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

// getColumnValue 获取列的值
func (b *BaseEngine) getColumnValue(expr ast.Expression, row []Cell, columns []ast.ColumnDefinition) (interface{}, error) {
	switch e := expr.(type) {
	case *ast.Identifier:
		// 处理表名.列名的形式
		parts := strings.Split(e.Value, ".")
		columnName := e.Value

		// 如果有表名前缀，只使用列名部分进行查找
		if len(parts) == 2 {
			columnName = parts[1]
		}

		// 查找列索引
		for i, col := range columns {
			if col.Name == columnName {
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
		return e.Value, nil // 直接返回已解析的值
	case *ast.StringLiteral:
		return e.Value, nil
	case *ast.FloatLiteral:
		return e.Value, nil // 直接返回已解析的值
	case *ast.DateTimeLiteral:
		return e.Value, nil // 直接返回已解析的值
	default:
		return nil, fmt.Errorf("Unknown expression type: %T", expr)
	}
}

// convertToCell 类型转换
func (b *BaseEngine) convertToCell(value interface{}, columnType string) Cell {
	switch v := value.(type) {
	case int32:
		return Cell{Type: CellTypeInt, IntValue: v}
	case float32:
		return Cell{Type: CellTypeFloat, FloatValue: v}
	case string:
		if columnType == "INT" {
			if intVal, err := strconv.ParseInt(v, 10, 32); err == nil {
				return Cell{Type: CellTypeInt, IntValue: int32(intVal)}
			}
		}
		return Cell{Type: CellTypeText, TextValue: v}
	case time.Time:
		return Cell{Type: CellTypeDateTime, TimeValue: v.Format("2006-01-02 15:04:05")}
	default:
		return Cell{Type: CellTypeText, TextValue: fmt.Sprintf("%v", v)}
	}
}

// evaluateWhereCondition 评估WHERE条件
func (b *BaseEngine) evaluateWhereCondition(expr ast.Expression, row []Cell, columns []ast.ColumnDefinition) (bool, error) {
	switch e := expr.(type) {
	case *ast.BinaryExpression:
		// 获取左操作数的值
		leftValue, err := b.getColumnValue(e.Left, row, columns)
		if err != nil {
			return false, err
		}

		// 获取右操作数的值
		rightValue, err := b.getColumnValue(e.Right, row, columns)
		if err != nil {
			return false, err
		}

		// 根据操作符比较值
		return b.compareValues(leftValue, rightValue, e.Operator)
	case *ast.LikeExpression:
		// 获取左操作数的值
		leftValue, err := b.getColumnValue(e.Left, row, columns)
		if err != nil {
			return false, err
		}

		// 确保左操作数是字符串类型
		strValue, ok := leftValue.(string)
		if !ok {
			return false, fmt.Errorf("LIKE operator requires string operand")
		}

		// 执行LIKE匹配
		return b.matchLikePattern(strValue, e.Pattern), nil
	case *ast.BetweenExpression:
		// 解析需要比较的字段（between左侧的字段）
		colIndex, err := getColumnIndex(e.Left.(*ast.Identifier).Value, columns)
		if err != nil {
			return false, err
		}

		// 获取列值
		left := row[colIndex]
		lower, err := b.evaluateExpression(e.Low)
		if err != nil {
			return false, err
		}

		upper, err := b.evaluateExpression(e.High)
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

// getCellValue 从 Cell 中提取实际值
func getCellValue(cell Cell) interface{} {
	switch cell.Type {
	case CellTypeInt:
		return cell.IntValue
	case CellTypeText:
		return cell.TextValue
	case CellTypeFloat:
		return cell.FloatValue
	case CellTypeDateTime:
		val, err := time.Parse("2006-01-02 15:04:05", cell.TimeValue)
		if err != nil {
			return cell.TimeValue
		}
		return val
	default:
		return cell.String()
	}
}

// isNumericType 检查是否为数字类型
func isNumericType(v interface{}) bool {
	switch v.(type) {
	case int32, float32:
		return true
	default:
		return false
	}
}

// compareNumericValues 比较数字类型值
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

// isEqual 判断是否相等
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

// isGreater 判断是否大于
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

// isLess 判断是否小于
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

// getColumnIndex 根据列名获取列索引
func getColumnIndex(columnName string, columns []ast.ColumnDefinition) (int, error) {
	for i, col := range columns {
		if col.Name == columnName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column '%s' not found", columnName)
}
