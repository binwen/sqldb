package clause_test

import (
	"fmt"
	"testing"

	"sqldb/clause"
)

func TestInsert(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{
				clause.Insert{},
				clause.Values{
					Columns: []clause.Column{{Name: "name"}, {Name: "age"}},
					Values:  [][]interface{}{{"bin", 18}, {"wen", 1}},
				},
			},
			"INSERT INTO `user` (`name`,`age`) VALUES (?,?),(?,?)",
			[]interface{}{"bin", 18, "wen", 1},
		},
		{
			[]clause.IClause{
				clause.Insert{Modifier: "LOW_PRIORITY"},
				clause.Values{
					Columns: []clause.Column{{Name: "name"}, {Name: "age"}},
					Values:  [][]interface{}{{"bin", 18}, {"wen", 1}},
				},
			},
			"INSERT LOW_PRIORITY INTO `user` (`name`,`age`) VALUES (?,?),(?,?)",
			[]interface{}{"bin", 18, "wen", 1},
		},
		{
			[]clause.IClause{
				clause.Insert{Table: clause.Table{Name: "products"}, Modifier: "LOW_PRIORITY"},
				clause.Values{
					Columns: []clause.Column{{Name: "name"}, {Name: "week"}},
					Values:  [][]interface{}{{"java", 18}, {"python", 1}},
				},
			},
			"INSERT LOW_PRIORITY INTO `products` (`name`,`week`) VALUES (?,?),(?,?)",
			[]interface{}{"java", 18, "python", 1},
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
