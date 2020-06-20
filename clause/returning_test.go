package clause_test

import (
	"fmt"
	"testing"

	"github.com/binwen/sqldb/clause"
)

func TestReturning(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{
				clause.Insert{},
				clause.Values{Columns: []clause.Column{{Name: "name"}, {Name: "age"}}, Values: [][]interface{}{{"bin", 18}}},
				clause.Returning{},
			},
			"INSERT INTO `user` (`name`,`age`) VALUES (?,?) RETURNING *",
			[]interface{}{"bin", 18},
		},
		{
			[]clause.IClause{
				clause.Insert{},
				clause.Values{Columns: []clause.Column{{Name: "name"}, {Name: "age"}}, Values: [][]interface{}{{"bin", 18}}},
				clause.Returning{Columns: []clause.Column{{Name: "id"}}},
				clause.Returning{Columns: []clause.Column{{Name: "name"}}},
			},
			"INSERT INTO `user` (`name`,`age`) VALUES (?,?) RETURNING `id`,`name`",
			[]interface{}{"bin", 18},
		},
		{
			[]clause.IClause{
				clause.Update{},
				clause.Set{Assignments: []clause.Assignment{{clause.Column{Table: clause.CurrentTable, Name: "id"}, 1}}},
				clause.Returning{Columns: []clause.Column{{Name: "id"}}},
			},
			"UPDATE `user` SET `user`.`id`=? RETURNING `id`",
			[]interface{}{1},
		},
		{
			[]clause.IClause{
				clause.Update{},
				clause.Set{Assignments: []clause.Assignment{{clause.Column{Table: clause.CurrentTable, Name: "id"}, 1}}},
				clause.Returning{Columns: []clause.Column{{Name: "id"}, {Name: "name"}}},
			},
			"UPDATE `user` SET `user`.`id`=? RETURNING `id`,`name`",
			[]interface{}{1},
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
