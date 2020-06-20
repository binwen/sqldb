package clause

import (
	"database/sql/driver"
	"reflect"
)

type Expression interface {
	Build(builder Builder)
}

type NegationExpressionBuilder interface {
	NegationBuild(builder Builder)
}

type Expr struct {
	SQL  string
	Vars []interface{}
}

func (expr Expr) Build(builder Builder) {
	var (
		idx         int
		hasBrackets bool
	)
	for _, v := range []byte(expr.SQL) {
		if v == '?' {
			if hasBrackets {
				if _, ok := expr.Vars[idx].(driver.Valuer); ok {
					builder.AddSQLVar(builder, expr.Vars[idx])
				} else {
					switch rv := reflect.ValueOf(expr.Vars[idx]); rv.Kind() {
					case reflect.Slice, reflect.Array:
						for i := 0; i < rv.Len(); i++ {
							if i > 0 {
								builder.WriteByte(',')
							}
							builder.AddSQLVar(builder, rv.Index(i).Interface())
						}
					default:
						builder.AddSQLVar(builder, expr.Vars[idx])
					}
				}
			} else {
				builder.AddSQLVar(builder, expr.Vars[idx])
			}

			idx++
		} else {
			if v == '(' {
				hasBrackets = true
			} else {
				hasBrackets = false
			}
			builder.WriteByte(v)
		}
	}
}

func isSlice(value interface{}) bool {
	switch rv := reflect.Indirect(reflect.ValueOf(value)); rv.Kind() {
	case reflect.Slice, reflect.Array:
		return true
	}

	return false
}

type EQ struct {
	Column interface{}
	Value  interface{}
}

func (eq EQ) Build(builder Builder) {
	builder.WriteQuoted(eq.Column)

	if eq.Value == nil {
		builder.WriteString(" IS NULL")
	} else if isSlice(eq.Value) {
		builder.WriteString(" IN ")
		builder.AddSQLVar(builder, eq.Value)
	} else {
		builder.WriteString(" = ")
		builder.AddSQLVar(builder, eq.Value)
	}
}

func (eq EQ) NegationBuild(builder Builder) {
	NEQ{eq.Column, eq.Value}.Build(builder)
}

type NEQ EQ

func (neq NEQ) Build(builder Builder) {
	builder.WriteQuoted(neq.Column)

	if neq.Value == nil {
		builder.WriteString(" IS NOT NULL")
	} else if isSlice(neq.Value) {
		builder.WriteString(" NOT IN ")
		builder.AddSQLVar(builder, neq.Value)
	} else {
		builder.WriteString(" <> ")
		builder.AddSQLVar(builder, neq.Value)
	}
}

func (neq NEQ) NegationBuild(builder Builder) {
	EQ{neq.Column, neq.Value}.Build(builder)
}

type Gt EQ

func (gt Gt) Build(builder Builder) {
	builder.WriteQuoted(gt.Column)
	builder.WriteString(" > ")
	builder.AddSQLVar(builder, gt.Value)
}

func (gt Gt) NegationBuild(builder Builder) {
	Lte{gt.Column, gt.Value}.Build(builder)
}

// Gte greater than or equal to for where
type Gte EQ

func (gte Gte) Build(builder Builder) {
	builder.WriteQuoted(gte.Column)
	builder.WriteString(" >= ")
	builder.AddSQLVar(builder, gte.Value)
}

func (gte Gte) NegationBuild(builder Builder) {
	Lt{gte.Column, gte.Value}.Build(builder)
}

// Lt less than for where
type Lt EQ

func (lt Lt) Build(builder Builder) {
	builder.WriteQuoted(lt.Column)
	builder.WriteString(" < ")
	builder.AddSQLVar(builder, lt.Value)
}

func (lt Lt) NegationBuild(builder Builder) {
	Gte{lt.Column, lt.Value}.Build(builder)
}

// Lte less than or equal to for where
type Lte EQ

func (lte Lte) Build(builder Builder) {
	builder.WriteQuoted(lte.Column)
	builder.WriteString(" <= ")
	builder.AddSQLVar(builder, lte.Value)
}

func (lte Lte) NegationBuild(builder Builder) {
	Gt{lte.Column, lte.Value}.Build(builder)
}

// Like whether string matches regular expression
type Like EQ

func (like Like) Build(builder Builder) {
	builder.WriteQuoted(like.Column)
	builder.WriteString(" LIKE ")
	builder.AddSQLVar(builder, like.Value)
}

func (like Like) NegationBuild(builder Builder) {
	builder.WriteQuoted(like.Column)
	builder.WriteString(" NOT LIKE ")
	builder.AddSQLVar(builder, like.Value)
}

type IN struct {
	Column interface{}
	Values []interface{}
}

func (in IN) Build(builder Builder) {
	builder.WriteQuoted(in.Column)

	switch len(in.Values) {
	case 0:
		builder.WriteString(" IN (NULL)")
	case 1:
		builder.WriteString(" = ")
		builder.AddSQLVar(builder, in.Values...)
	default:
		builder.WriteString(" IN (")
		builder.AddSQLVar(builder, in.Values...)
		builder.WriteByte(')')
	}
}

func (in IN) NegationBuild(builder Builder) {
	switch len(in.Values) {
	case 0:
	case 1:
		builder.WriteQuoted(in.Column)
		builder.WriteString(" <> ")
		builder.AddSQLVar(builder, in.Values...)
	default:
		builder.WriteQuoted(in.Column)
		builder.WriteString(" NOT IN (")
		builder.AddSQLVar(builder, in.Values...)
		builder.WriteByte(')')
	}
}
