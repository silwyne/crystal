package operation

import (
	"github.com/crystal/api/row"
)

type Transformation interface {
	Apply(source chan row.Row, result chan row.Row)
	IsResultStateless() bool
	GetName() string
}
