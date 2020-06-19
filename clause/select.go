package clause

type Select struct {
	Columns     []Column
	Expressions []Expression
	Distinct    bool
}

func (s Select) Name() string {
	return "SELECT"
}

func (s Select) Build(builder Builder) {
	columnCount := len(s.Columns)
	if columnCount > 0 {
		if s.Distinct {
			builder.WriteString("DISTINCT ")
		}
		for idx, column := range s.Columns {
			if idx > 0 {
				builder.WriteByte(',')
			}
			builder.WriteQuoted(column)
		}
	}

	for idx, expr := range s.Expressions {
		if columnCount > 0 || idx > 0 {
			builder.WriteByte(',')
		}
		expr.Build(builder)
	}
	if columnCount == 0 && len(s.Expressions) == 0 {
		builder.WriteByte('*')
	}
}

func (s Select) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(Select); ok {
		if !s.Distinct && v.Distinct {
			s.Distinct = v.Distinct
		}
		s.Expressions = append(v.Expressions, s.Expressions...)
		s.Columns = append(v.Columns, s.Columns...)
	}
	clause.Expression = s

}
