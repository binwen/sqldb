package clause_test

import (
	"fmt"
	"testing"

	"sqldb/clause"
)

func TestWhere(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Where{
					Exprs: []clause.Expression{
						clause.EQ{Column: "id", Value: "1"},
						clause.Gt{Column: "age", Value: 18},
						clause.Or(clause.NEQ{Column: "name", Value: "jinzhu"}),
					},
				},
			},
			"SELECT * FROM `user` WHERE `id` = ? AND `age` > ? OR `name` <> ?",
			[]interface{}{"1", 18, "jinzhu"},
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Where{
					Exprs: []clause.Expression{
						clause.Or(clause.NEQ{Column: "name", Value: "jinzhu"}),
						clause.EQ{Column: "id", Value: "1"},
						clause.Gt{Column: "age", Value: 18},
					},
				},
			},
			"SELECT * FROM `user` WHERE `id` = ? OR `name` <> ? AND `age` > ?",
			[]interface{}{"1", "jinzhu", 18},
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Where{
					Exprs: []clause.Expression{
						clause.Or(clause.NEQ{Column: "name", Value: "jinzhu"}),
						clause.EQ{Column: "id", Value: "1"},
						clause.Gt{Column: "age", Value: 18},
					},
				},
			},
			"SELECT * FROM `user` WHERE `id` = ? OR `name` <> ? AND `age` > ?",
			[]interface{}{"1", "jinzhu", 18},
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Where{
					Exprs: []clause.Expression{
						clause.Or(clause.EQ{Column: "id", Value: "1"}),
						clause.Or(clause.NEQ{Column: "name", Value: "jinzhu"}),
					},
				},
			},
			"SELECT * FROM `user` WHERE `id` = ? OR `name` <> ?",
			[]interface{}{"1", "jinzhu"},
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Where{
					Exprs: []clause.Expression{
						clause.EQ{Column: "id", Value: "1"},
						clause.Gt{Column: "age", Value: 18},
						clause.Or(clause.NEQ{Column: "name", Value: "jinzhu"}),
					},
				},
				clause.Where{
					Exprs: []clause.Expression{
						clause.Or(clause.Gt{Column: "score", Value: 100},
							clause.Like{Column: "name", Value: "%linus%"}),
					},
				},
			},
			"SELECT * FROM `user` WHERE `id` = ? AND `age` > ? OR `name` <> ? AND (`score` > ? OR `name` LIKE ?)",
			[]interface{}{"1", 18, "jinzhu", 100, "%linus%"},
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Where{
					Exprs: []clause.Expression{
						clause.Not(clause.EQ{Column: "id", Value: "1"},
							clause.Gt{Column: "age", Value: 18}),
						clause.Or(clause.NEQ{Column: "name", Value: "jinzhu"}),
					},
				},
				clause.Where{
					Exprs: []clause.Expression{
						clause.Or(clause.Not(clause.Gt{Column: "score", Value: 100}),
							clause.Like{Column: "name", Value: "%linus%"}),
					},
				},
			},
			"SELECT * FROM `user` WHERE (`id` <> ? AND `age` <= ?) OR `name` <> ? AND (`score` <= ? OR `name` LIKE ?)",
			[]interface{}{"1", 18, "jinzhu", 100, "%linus%"},
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
