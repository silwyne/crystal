package operation

import (
	"sync"

	"github.com/crystal/api/row"
)

type Transformation interface {
	Execute(wg *sync.WaitGroup, source chan row.Row) chan row.Row
	IsResultStateless() bool
	GetName() string
}
