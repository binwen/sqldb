package clause_test

import (
	"fmt"
	"testing"

	"sqldb/clause"
)

func TestGroupBy(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.GroupBy{
					Columns: []clause.Column{{Name: "role"}},
					Having:  []clause.Expression{clause.EQ{Column: "role", Value: "admin"}},
				},
			},
			"SELECT * FROM `user` GROUP BY `role` HAVING `role` = ?",
			[]interface{}{"admin"},
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.GroupBy{
					Columns: []clause.Column{{Name: "role"}},
					Having:  []clause.Expression{clause.EQ{Column: "role", Value: "admin"}},
				},
				clause.GroupBy{
					Columns: []clause.Column{{Name: "gender"}},
					Having:  []clause.Expression{clause.NEQ{Column: "gender", Value: "U"}},
				},
			},
			"SELECT * FROM `user` GROUP BY `role`,`gender` HAVING `role` = ? AND `gender` <> ?",
			[]interface{}{"admin", "U"},
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
