package clause

import "sort"

type Update struct {
	Modifier string
	Table    Table
}

func (update Update) Name() string {
	return "UPDATE"
}

func (update Update) Build(builder Builder) {
	if update.Modifier != "" {
		builder.WriteString(update.Modifier)
		builder.WriteByte(' ')
	}

	if update.Table.Name == "" {
		builder.WriteQuoted(currentTable)
	} else {
		builder.WriteQuoted(update.Table)
	}
}

func (update Update) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(Update); ok {
		if update.Modifier == "" {
			update.Modifier = v.Modifier
		}
		if update.Table.Name == "" {
			update.Table = v.Table
		}
	}
	clause.Expression = update
}

type Assignment struct {
	Column Column
	Value  interface{}
}

type Set struct {
	Assignments []Assignment
}

func (set Set) Name() string {
	return "SET"
}

func (set Set) Build(builder Builder) {
	for idx, assignment := range set.Assignments {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteQuoted(assignment.Column)
		builder.WriteByte('=')
		builder.AddSQLVar(builder, assignment.Value)
	}
}

func (set Set) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(Set); ok {
		set.Assignments = append(v.Assignments, set.Assignments...)
	}

	clause.Expression = set
}

func Assignments(values map[string]interface{}) Set {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	assignments := make([]Assignment, len(keys))
	for idx, key := range keys {
		assignments[idx] = Assignment{Column: Column{Name: key}, Value: values[key]}
	}

	return Set{Assignments: assignments}
}
