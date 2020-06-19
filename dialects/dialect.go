package dialects

import (
	"github.com/jmoiron/sqlx"

	"sqldb/clause"
)

var dialectMapping = map[string]Dialector{}

type Queryer interface {
	QueryRow(query string, args ...interface{}) *sqlx.Row
}

type Dialector interface {
	SetQueryer(queryer Queryer)
	QuoteTo(clause.Writer, string)
	BindVarTo(writer clause.Writer, varIndex int, v interface{})
	PKColumnNames(table string) []string
	LastInsertIDReversed() bool
	WithReturning() bool
}

func RegisterDialector(name string, dialect Dialector) {
	dialectMapping[name] = dialect
}

func GetDialector(name string) (dialect Dialector, ok bool) {
	dialect, ok = dialectMapping[name]
	return
}
