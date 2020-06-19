package clause

type IClause interface {
	Name() string
	Build(Builder)
	MergeClause(*Clause)
}

type Writer interface {
	WriteByte(byte) error
	WriteString(string) (int, error)
}

type Builder interface {
	Writer
	WriteQuoted(field interface{}) error
	AddSQLVar(Writer, ...interface{})
}

type ClauseBuilder interface {
	Build(Clause, Builder)
}

type Clause struct {
	Name                 string
	Priority             float64
	BeforeExpressions    []Expression
	AfterNameExpressions []Expression
	AfterExpressions     []Expression
	Expression           Expression
	Builder              ClauseBuilder
}

func (c Clause) Build(builder Builder) {
	if c.Builder != nil {
		c.Builder.Build(c, builder)
	} else {
		builders := c.BeforeExpressions
		if c.Name != "" {
			builders = append(builders, Expr{SQL: c.Name})
		}

		builders = append(builders, c.AfterNameExpressions...)
		if c.Expression != nil {
			builders = append(builders, c.Expression)
		}

		for idx, expr := range append(builders, c.AfterExpressions...) {
			if idx != 0 {
				builder.WriteByte(' ')
			}
			expr.Build(builder)
		}
	}
}

type Table struct {
	Name  string
	Alias string
	Raw   bool
}

type Column struct {
	Table string
	Name  string
	Alias string
	Raw   bool
}

const CurrentTable string = "@@@table@@@"

var currentTable = Table{Name: CurrentTable}
