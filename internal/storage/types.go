// internal/storage/types.go
package storage

import "fmt"

// Cell 表示单元格
type Cell struct {
	Type       CellType
	IntValue   int32
	TextValue  string
	FloatValue float32
	TimeValue  string
}

// CellType 表示单元格类型
type CellType int

const (
	CellTypeInt CellType = iota
	CellTypeText
	CellTypeFloat
	CellTypeDateTime
)

// String 返回单元格的字符串表示
func (c Cell) String() string {
	switch c.Type {
	case CellTypeInt:
		return fmt.Sprintf("%d", c.IntValue)
	case CellTypeText:
		return c.TextValue
	case CellTypeFloat:
		return fmt.Sprintf("%.2f", c.FloatValue)
	case CellTypeDateTime:
		return c.TimeValue
	default:
		return "NULL"
	}
}
