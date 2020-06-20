package clause

type OnConflict struct {
	Columns   []Column
	Where     Where
	DoNothing bool
	DoUpdates Set
}

func (OnConflict) Name() string {
	return "ON CONFLICT"
}

func (onConflict OnConflict) Build(builder Builder) {
	if len(onConflict.Columns) > 0 {
		builder.WriteByte('(')
		for idx, column := range onConflict.Columns {
			if idx > 0 {
				builder.WriteByte(',')
			}
			builder.WriteQuoted(column)
		}
		builder.WriteString(`) `)
	}

	if len(onConflict.Where.Exprs) > 0 {
		builder.WriteString("WHERE ")
		onConflict.Where.Build(builder)
		builder.WriteByte(' ')
	}

	if onConflict.DoNothing {
		builder.WriteString("DO NOTHING")
	} else {
		builder.WriteString("DO UPDATE SET ")
		onConflict.DoUpdates.Build(builder)
	}
}

func (onConflict OnConflict) MergeClause(clause *Clause) {
	clause.Expression = onConflict
}
