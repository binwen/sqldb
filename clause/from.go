package clause

const (
	CrossJoin JoinType = "CROSS"
	InnerJoin          = "INNER"
	LeftJoin           = "LEFT"
	RightJoin          = "RIGHT"
)

type From struct {
	Tables []Table
	Joins  []Join
}

func (f From) Name() string {
	return "FROM"
}

func (f From) Build(builder Builder) {
	if len(f.Tables) == 0 {
		builder.WriteQuoted(currentTable)
	} else {
		for idx, table := range f.Tables {
			if idx > 0 {
				builder.WriteByte(',')
			}

			builder.WriteQuoted(table)
		}
	}

	for _, join := range f.Joins {
		builder.WriteByte(' ')
		join.Build(builder)
	}
}

func (f From) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(From); ok {
		f.Tables = append(v.Tables, f.Tables...)
		f.Joins = append(v.Joins, f.Joins...)
	}
	clause.Expression = f
}

type JoinType string

type Join struct {
	Type       JoinType
	Table      Table
	ON         Where
	Using      []string
	Expression Expression
}

func (j Join) Build(builder Builder) {
	if j.Expression != nil {
		j.Expression.Build(builder)
	} else {
		if j.Type != "" {
			builder.WriteString(string(j.Type))
			builder.WriteByte(' ')
		}

		builder.WriteString("JOIN ")
		builder.WriteQuoted(j.Table)

		if len(j.ON.Exprs) > 0 {
			builder.WriteString(" ON ")
			j.ON.Build(builder)
		} else if len(j.Using) > 0 {
			builder.WriteString(" USING (")
			for idx, c := range j.Using {
				if idx > 0 {
					builder.WriteByte(',')
				}
				builder.WriteQuoted(c)
			}
			builder.WriteByte(')')
		}
	}
}
