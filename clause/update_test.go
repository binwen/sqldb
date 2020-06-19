package clause_test

import (
	"fmt"
	"testing"

	"sqldb/clause"
)

func TestUpdate(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{
				clause.Update{},
				clause.Set{Assignments: []clause.Assignment{{clause.Column{Table: clause.CurrentTable, Name: "id"}, 1}}},
			},
			"UPDATE `user` SET `user`.`id`=?",
			[]interface{}{1},
		},
		{
			[]clause.IClause{
				clause.Update{Modifier: "LOW_PRIORITY"},
				clause.Set{Assignments: []clause.Assignment{{clause.Column{Table: clause.CurrentTable, Name: "id"}, 1}}},
			},
			"UPDATE LOW_PRIORITY `user` SET `user`.`id`=?",
			[]interface{}{1},
		},
		{
			[]clause.IClause{
				clause.Update{Table: clause.Table{Name: "products"}, Modifier: "LOW_PRIORITY"},
				clause.Set{Assignments: []clause.Assignment{{clause.Column{Name: "id"}, 1}}},
				clause.Set{Assignments: []clause.Assignment{{clause.Column{Name: "name"}, "java"}}},
			},
			"UPDATE LOW_PRIORITY `products` SET `id`=?,`name`=?",
			[]interface{}{1, "java"},
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
