package clause_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/binwen/sqldb"
	"github.com/binwen/sqldb/clause"
	"github.com/binwen/sqldb/dialects"
)

type DummyDialector struct {
}

func (dia *DummyDialector) SetQueryer(queryer dialects.Queryer) {

}

func (dia *DummyDialector) LastInsertIDReversed() bool {
	return false
}

func (dia *DummyDialector) WithReturning() bool {
	return false
}

func (dia *DummyDialector) BindVarTo(writer clause.Writer, varIndex int, v interface{}) {
	writer.WriteByte('?')
}

func (dia *DummyDialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	writer.WriteString(str)
	writer.WriteByte('`')
}

func (dia *DummyDialector) PKColumnNames(table string) (columnNames []string) {
	return
}

func checkBuildClauses(t *testing.T, clauses []clause.IClause, result string, vars []interface{}) {
	dialector := DummyDialector{}

	var (
		buildNames    []string
		buildNamesMap = map[string]bool{}
		stmt          = sqldb.Statement{Dialector: &dialector, Tables: []clause.Table{clause.Table{Name: "user"}}, Clauses: map[string]clause.Clause{}}
	)

	for _, c := range clauses {
		if _, ok := buildNamesMap[c.Name()]; !ok {
			buildNames = append(buildNames, c.Name())
			buildNamesMap[c.Name()] = true
		}

		stmt.AddClause(c)
	}

	stmt.Build(buildNames...)

	if strings.TrimSpace(stmt.SQL.String()) != result {
		t.Errorf("SQL expects %v got %v", result, stmt.SQL.String())
	}

	if !reflect.DeepEqual(stmt.SQLVars, vars) {
		t.Errorf("Vars expects %+v got %v", stmt.SQLVars, vars)
	}
}
