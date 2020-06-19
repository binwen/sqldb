package clause

type Insert struct {
	Table    Table
	Modifier string
}

func (insert Insert) Name() string {
	return "INSERT"
}

func (insert Insert) Build(builder Builder) {
	if insert.Modifier != "" {
		builder.WriteString(insert.Modifier)
		builder.WriteByte(' ')
	}

	builder.WriteString("INTO ")
	if insert.Table.Name == "" {
		builder.WriteQuoted(currentTable)
	} else {
		builder.WriteQuoted(insert.Table)
	}
}

func (insert Insert) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(Insert); ok {
		if insert.Modifier == "" {
			insert.Modifier = v.Modifier
		}
		if insert.Table.Name == "" {
			insert.Table = v.Table
		}
	}
	clause.Expression = insert
}

type Values struct {
	Columns []Column
	Values  [][]interface{}
}

func (Values) Name() string {
	return "VALUES"
}

func (values Values) Build(builder Builder) {
	if len(values.Columns) == 0 {
		return
	}

	builder.WriteByte('(')
	for idx, column := range values.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteQuoted(column)
	}
	builder.WriteByte(')')

	builder.WriteString(" VALUES ")

	for idx, value := range values.Values {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('(')
		builder.AddSQLVar(builder, value...)
		builder.WriteByte(')')
	}
}

func (values Values) MergeClause(clause *Clause) {
	clause.Name = ""
	if v, ok := clause.Expression.(Values); ok {
		values.Values = append(v.Values, values.Values...)
	}
	clause.Expression = values
}
