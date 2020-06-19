package clause_test

import (
	"fmt"
	"testing"

	"sqldb/clause"
)

func TestSelect(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{clause.Select{}, clause.From{}},
			"SELECT * FROM `user`", nil,
		},
		{
			[]clause.IClause{clause.Select{
				Columns: []clause.Column{clause.Column{Table: "user", Name: "id"}},
			}, clause.From{}},
			"SELECT `user`.`id` FROM `user`", nil,
		},
		{
			[]clause.IClause{
				clause.Select{Columns: []clause.Column{clause.Column{Name: "id"}}},
				clause.Select{Columns: []clause.Column{{Name: "name"}}},
				clause.From{},
				clause.Select{Distinct: true},
			},
			"SELECT DISTINCT `id`,`name` FROM `user`", nil,
		},
		{
			[]clause.IClause{clause.Select{
				Columns: []clause.Column{clause.Column{Name: "id"}, {Name: "name"}},
			}, clause.From{}},
			"SELECT `id`,`name` FROM `user`", nil,
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
