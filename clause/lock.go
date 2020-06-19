package clause

type Lock struct {
	Strength string
	Table    Table
	Options  string
}

type For struct {
	Locks []Lock
}

func (f For) Name() string {
	return "FOR"
}

func (f For) Build(builder Builder) {
	for idx, locking := range f.Locks {
		if idx > 0 {
			builder.WriteByte(' ')
		}

		builder.WriteString("FOR ")
		builder.WriteString(locking.Strength)
		if locking.Table.Name != "" {
			builder.WriteString(" OF ")
			builder.WriteQuoted(locking.Table)
		}

		if locking.Options != "" {
			builder.WriteByte(' ')
			builder.WriteString(locking.Options)
		}
	}
}

func (f For) MergeClause(clause *Clause) {
	clause.Name = ""

	if v, ok := clause.Expression.(For); ok {
		f.Locks = append(v.Locks, f.Locks...)
	}

	clause.Expression = f
}
