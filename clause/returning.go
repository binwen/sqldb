package clause

type Returning struct {
	Columns []Column
}

func (r Returning) Name() string {
	return "RETURNING"
}

func (r Returning) Build(builder Builder) {
	if len(r.Columns) == 0 {
		builder.WriteByte('*')
		return
	}

	for idx, column := range r.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}

		builder.WriteQuoted(column)
	}
}

func (r Returning) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(Returning); ok {
		r.Columns = append(v.Columns, r.Columns...)
	}

	clause.Expression = r
}
