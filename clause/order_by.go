package clause

type OrderByColumn struct {
	Column Column
	Desc   bool
}

type OrderBy struct {
	Columns []OrderByColumn
}

func (o OrderBy) Name() string {
	return "ORDER BY"
}

func (o OrderBy) Build(builder Builder) {
	for idx, column := range o.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteQuoted(column.Column)
		if column.Desc {
			builder.WriteString(" DESC")
		}

	}

}

func (o OrderBy) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(OrderBy); ok {
		o.Columns = append(v.Columns, o.Columns...)
	}

	clause.Expression = o
}
