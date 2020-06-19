package clause_test

import (
	"fmt"
	"testing"

	"sqldb/clause"
)

func TestOrderBy(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.OrderBy{
					Columns: []clause.OrderByColumn{
						{Column: clause.Column{Table: "user", Name: "id"}, Desc: true},
					},
				},
			},
			"SELECT * FROM `user` ORDER BY `user`.`id` DESC",
			nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.OrderBy{
					Columns: []clause.OrderByColumn{
						{Column: clause.Column{Table: "user", Name: "id"}, Desc: false},
					},
				},
			},
			"SELECT * FROM `user` ORDER BY `user`.`id`",
			nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.OrderBy{
					Columns: []clause.OrderByColumn{{Column: clause.Column{Table: "user", Name: "id"}, Desc: true}},
				},
				clause.OrderBy{
					Columns: []clause.OrderByColumn{{Column: clause.Column{Name: "name"}}},
				},
			},
			"SELECT * FROM `user` ORDER BY `user`.`id` DESC,`name`",
			nil,
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
