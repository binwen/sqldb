package clause

type GroupBy struct {
	Columns []Column
	Having  []Expression
}

func (g GroupBy) Name() string {
	return "GROUP BY"
}

func (g GroupBy) Build(builder Builder) {
	for idx, column := range g.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}

		builder.WriteQuoted(column)
	}

	if len(g.Having) > 0 {
		builder.WriteString(" HAVING ")
		Where{Exprs: g.Having}.Build(builder)
	}
}

func (g GroupBy) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(GroupBy); ok {
		g.Columns = append(v.Columns, g.Columns...)
		g.Having = append(v.Having, g.Having...)
	}
	clause.Expression = g
}
