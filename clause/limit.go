package clause

import "strconv"

type Limit struct {
	Limit  int
	Offset int
}

func (l Limit) Name() string {
	return "LIMIT"
}

func (l Limit) Build(builder Builder) {
	if l.Limit > 0 {
		builder.WriteString("LIMIT ")
		builder.WriteString(strconv.Itoa(l.Limit))

		if l.Offset > 0 {
			builder.WriteString(" OFFSET ")
			builder.WriteString(strconv.Itoa(l.Offset))
		}
	}
}

func (l Limit) MergeClause(clause *Clause) {
	clause.Name = ""

	if v, ok := clause.Expression.(Limit); ok {
		if l.Limit == 0 && v.Limit > 0 {
			l.Limit = v.Limit
		} else if l.Limit < 0 {
			l.Limit = 0
		}

		if l.Offset == 0 && v.Offset > 0 {
			l.Offset = v.Offset
		} else if l.Offset < 0 {
			l.Offset = 0
		}
	}

	clause.Expression = l
}
