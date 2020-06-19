package sqldb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"sqldb/clause"
	"sqldb/dialects"
)

type Statement struct {
	Dialector dialects.Dialector
	Tables    []clause.Table
	Clauses   map[string]clause.Clause
	SQL       strings.Builder
	SQLVars   []interface{}
	NamedVars []sql.NamedArg
	Hint      string // 用于指定数据库中间件的命令，比如 /*+TDDL:slave()*/
}

// 添加子句
func (stmt *Statement) AddClause(v clause.IClause) {
	c, ok := stmt.Clauses[v.Name()]
	if !ok {
		c.Name = v.Name()
	}
	v.MergeClause(&c)
	stmt.Clauses[v.Name()] = c
}

// 如果子句不存在则添加
func (stmt *Statement) AddClauseIfNotExists(v clause.IClause) {
	if _, ok := stmt.Clauses[v.Name()]; !ok {
		stmt.AddClause(v)
	}
}

// 构建表达式
func (stmt *Statement) BuildCondition(query interface{}, args ...interface{}) (conditions []clause.Expression, err error) {
	if sqlStr, ok := query.(string); ok {
		if _, err := strconv.Atoi(sqlStr); err != nil {
			argsLen := len(args)
			if argsLen == 0 && sqlStr == "" {
				return conditions, nil
			} else if argsLen == 0 || strings.Contains(sqlStr, "@") {
				return []clause.Expression{clause.Expr{SQL: sqlStr, Vars: args}}, nil
			} else if argsLen > 0 && strings.Contains(sqlStr, "?") {
				return []clause.Expression{clause.Expr{SQL: ConvertInSQL(sqlStr), Vars: args}}, nil
			} else if argsLen == 1 {
				return []clause.Expression{clause.EQ{Column: sqlStr, Value: args[0]}}, nil
			}
		}
	}

	args = append([]interface{}{query}, args...)
	for _, arg := range args {
		if valuer, ok := arg.(driver.Valuer); ok {
			arg, _ = valuer.Value()
		}

		switch v := arg.(type) {
		case clause.Expression:
			conditions = append(conditions, v)
		default:
			reflectValue := reflect.Indirect(reflect.ValueOf(arg))
			switch reflectValue.Kind() {
			case reflect.Map:
				for _, k := range reflectValue.MapKeys() {
					conditions = append(conditions, clause.EQ{Column: k, Value: reflectValue.MapIndex(k).Interface()})
				}
			default:
				return conditions, fmt.Errorf("unsupported query args type: %v", reflectValue.Kind())
			}
		}
	}

	return conditions, nil
}

// 构建sql
func (stmt *Statement) Build(clauses ...string) {
	var firstClauseWritten bool
	if stmt.Hint != "" {
		stmt.WriteString(stmt.Hint)
	}
	for _, name := range clauses {
		if c, ok := stmt.Clauses[name]; ok {
			if firstClauseWritten {
				stmt.WriteByte(' ')
			}

			firstClauseWritten = true
			// if b, ok := stmt.DB.ClauseBuilders[name]; ok {
			// 	b.Build(c, stmt)
			// } else {
			c.Build(stmt)
			// }
		}
	}
}

func (stmt *Statement) WriteString(str string) (int, error) {
	return stmt.SQL.WriteString(str)
}

func (stmt *Statement) WriteByte(c byte) error {
	return stmt.SQL.WriteByte(c)
}

func (stmt *Statement) WriteQuoted(value interface{}) error {
	stmt.QuoteTo(&stmt.SQL, value)
	return nil
}

// 转换sql格式
func (stmt Statement) QuoteTo(writer clause.Writer, field interface{}) {
	switch v := field.(type) {
	case clause.Table:
		if v.Name == clause.CurrentTable {
			stmt.Dialector.QuoteTo(writer, stmt.Tables[0].Name)
		} else {
			stmt.Dialector.QuoteTo(writer, v.Name)
		}

		if v.Alias != "" {
			writer.WriteString(" AS ")
			stmt.Dialector.QuoteTo(writer, v.Alias)
		}
	case clause.Column:
		if v.Table != "" {
			if v.Table == clause.CurrentTable {
				stmt.Dialector.QuoteTo(writer, stmt.Tables[0].Name)
			} else {
				stmt.Dialector.QuoteTo(writer, v.Table)
			}
			writer.WriteByte('.')
		}

		if v.Raw {
			writer.WriteString(v.Name)
		} else {
			stmt.Dialector.QuoteTo(writer, v.Name)
		}

		if v.Alias != "" {
			writer.WriteString(" AS ")
			stmt.Dialector.QuoteTo(writer, v.Alias)
		}
	case string:
		stmt.Dialector.QuoteTo(writer, v)
	case []string:
		writer.WriteByte('(')
		for idx, d := range v {
			if idx != 0 {
				writer.WriteString(",")
			}
			stmt.Dialector.QuoteTo(writer, d)
		}
		writer.WriteByte(')')
	default:
		stmt.Dialector.QuoteTo(writer, fmt.Sprint(field))
	}
}

// Quote returns quoted value
func (stmt Statement) Quote(field interface{}) string {
	var builder strings.Builder
	stmt.QuoteTo(&builder, field)
	return builder.String()
}

// 添加sql占位符
func (stmt *Statement) AddSQLVar(writer clause.Writer, vars ...interface{}) {
	for idx, value := range vars {
		if idx > 0 {
			writer.WriteByte(',')
		}

		switch v := value.(type) {
		case sql.NamedArg:
			if len(v.Name) > 0 {
				stmt.NamedVars = append(stmt.NamedVars, v)
				writer.WriteByte('@')
				writer.WriteString(v.Name)
			} else {
				stmt.SQLVars = append(stmt.SQLVars, v.Value)
				stmt.Dialector.BindVarTo(writer, len(stmt.SQLVars), v.Value)
			}
		case clause.Column, clause.Table:
			stmt.QuoteTo(writer, v)
		case clause.Expr, *clause.Expr:
			expr, ok := v.(clause.Expr)
			if !ok {
				if v, ok := v.(*clause.Expr); ok {
					expr = *v
				}
			}
			writer.WriteString(expr.SQL)
			stmt.SQLVars = append(stmt.SQLVars, expr.Vars...)
		case driver.Valuer:
			stmt.SQLVars = append(stmt.SQLVars, v)
			stmt.Dialector.BindVarTo(writer, len(stmt.SQLVars), v)
		case []interface{}:
			if len(v) > 0 {
				writer.WriteByte('(')
				stmt.AddSQLVar(writer, v...)
				writer.WriteByte(')')
			} else {
				writer.WriteString("(NULL)")
			}
		default:
			switch rv := reflect.Indirect(reflect.ValueOf(v)); rv.Kind() {
			case reflect.Slice, reflect.Array:
				if rv.Len() == 0 {
					writer.WriteString("(NULL)")
				} else {
					writer.WriteByte('(')
					for i := 0; i < rv.Len(); i++ {
						if i > 0 {
							writer.WriteByte(',')
						}
						stmt.AddSQLVar(writer, rv.Index(i).Interface())
					}
					writer.WriteByte(')')
				}
			default:
				stmt.SQLVars = append(stmt.SQLVars, v)
				stmt.Dialector.BindVarTo(writer, len(stmt.SQLVars), v)
			}
		}
	}
}

func (stmt *Statement) ReInit() {
	stmt.Tables = nil
	stmt.SQL.Reset()
	stmt.SQLVars = nil
	stmt.NamedVars = nil
	stmt.Hint = ""

	for k := range stmt.Clauses {
		delete(stmt.Clauses, k)
	}
}
