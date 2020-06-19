package sqlite

import (
	_ "github.com/mattn/go-sqlite3"

	"sqldb/clause"
	"sqldb/dialects"
)

type Dialector struct {
	queryer              dialects.Queryer
	lastInsertIDReversed bool
	withReturning        bool
}

func init() {
	dialects.RegisterDialector("sqlite3", &Dialector{lastInsertIDReversed: true})
}

func (dia *Dialector) SetQueryer(queryer dialects.Queryer) {
	dia.queryer = queryer
}

func (dia *Dialector) LastInsertIDReversed() bool {
	return dia.lastInsertIDReversed
}

func (dia *Dialector) WithReturning() bool {
	return dia.withReturning
}

func (dia *Dialector) BindVarTo(writer clause.Writer, varIndex int, v interface{}) {
	writer.WriteByte('?')
}

func (dia *Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	writer.WriteString(str)
	writer.WriteByte('`')
}

func (dia *Dialector) PKColumnNames(table string) (columnNames []string) {
	return
}
