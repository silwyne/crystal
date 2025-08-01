package row

import (
	"fmt"
	"strings"
)

type Row struct {
	data []interface{}
}

func From(data ...interface{}) Row {
	return Row{data: data}
}

func (r *Row) AddColumn(in interface{}) *Row {
	r.data = append(r.data, in)
	return r
}

func (r *Row) GetFieldByPosition(position int) any {
	return r.data[position]
}

func (r Row) ToString() string {
	var sb strings.Builder
	for i, item := range r.data {
		sb.WriteString(fmt.Sprintf("%v", item))
		if i != len(r.data)-1 {
			sb.WriteString(",")
		}
	}

	return sb.String()
}
