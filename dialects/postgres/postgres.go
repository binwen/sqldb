package postgres

import (
	"strconv"
	"strings"

	_ "github.com/lib/pq"

	"github.com/binwen/sqldb/clause"
	"github.com/binwen/sqldb/dialects"
)

type Dialector struct {
	queryer              dialects.Queryer
	lastInsertIDReversed bool
	withReturning        bool
}

func init() {
	dialects.RegisterDialector("postgres", &Dialector{withReturning: true})
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
	writer.WriteByte('$')
	writer.WriteString(strconv.Itoa(varIndex))
}

func (dia *Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	writer.WriteString(str)
	writer.WriteByte('"')
}

func (dia *Dialector) PKColumnNames(table string) (columnNames []string) {
	sql := "SELECT indexdef FROM pg_indexes WHERE tablename=$1 and indexname in ($2,'primary') limit 1"
	var indexdef string
	_ = dia.queryer.QueryRow(sql, table, table+"_pkey").Scan(&indexdef)
	if indexdef == "" {
		return
	}

	for _, v := range strings.Split(strings.Split(strings.Split(indexdef, "(")[1], ")")[0], ",") {
		columnNames = append(columnNames, strings.Split(strings.TrimLeft(v, " "), " ")[0])
	}

	return
}
