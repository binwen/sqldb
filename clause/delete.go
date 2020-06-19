package clause

type Delete struct {
	Modifier string
}

func (delete Delete) Name() string {
	return "DELETE"
}

func (delete Delete) Build(builder Builder) {
	builder.WriteString("DELETE")

	if delete.Modifier != "" {
		builder.WriteByte(' ')
		builder.WriteString(delete.Modifier)
	}
}

func (delete Delete) MergeClause(clause *Clause) {
	clause.Name = ""
	clause.Expression = delete
}
