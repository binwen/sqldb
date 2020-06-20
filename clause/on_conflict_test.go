package clause_test

import (
	"fmt"
	"testing"

	"github.com/binwen/sqldb/clause"
)

func TestOnConflict(t *testing.T) {
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
					Values:  [][]interface{}{{"bin", 18}},
				},
				clause.OnConflict{
					Columns:   []clause.Column{{Name: "name"}},
					Where:     clause.Where{Exprs: []clause.Expression{clause.EQ{Column: "id", Value: 1}}},
					DoUpdates: clause.Assignments(map[string]interface{}{"name": "upsert-name"}),
				},
			},
			"INSERT INTO `user` (`name`,`age`) VALUES (?,?) ON CONFLICT (`name`) WHERE `id` = ? DO UPDATE SET `name`=?",
			[]interface{}{"bin", 18, 1, "upsert-name"},
		},
		{
			[]clause.IClause{
				clause.Insert{},
				clause.Values{
					Columns: []clause.Column{{Name: "name"}, {Name: "age"}},
					Values:  [][]interface{}{{"bin", 18}},
				},
				clause.OnConflict{DoNothing: true},
			},
			"INSERT INTO `user` (`name`,`age`) VALUES (?,?) ON CONFLICT DO NOTHING",
			[]interface{}{"bin", 18},
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
