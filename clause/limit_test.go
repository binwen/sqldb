package clause_test

import (
	"fmt"
	"testing"

	"github.com/binwen/sqldb/clause"
)

func TestLimit(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Limit{Limit: 10, Offset: 20},
			},
			"SELECT * FROM `user` LIMIT 10 OFFSET 20",
			nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Limit{Limit: 10, Offset: 20},
				clause.Limit{Offset: 30},
			},
			"SELECT * FROM `user` LIMIT 10 OFFSET 30",
			nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Limit{Limit: 10, Offset: 20},
				clause.Limit{Offset: 30},
				clause.Limit{Offset: -10},
			},
			"SELECT * FROM `user` LIMIT 10", nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Limit{Limit: 10, Offset: 20},
				clause.Limit{Offset: 30},
				clause.Limit{Limit: -10},
			},
			"SELECT * FROM `user`",
			nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.Limit{Limit: 10, Offset: 20},
				clause.Limit{Offset: 30},
				clause.Limit{Limit: 50},
			},
			"SELECT * FROM `user` LIMIT 50 OFFSET 30",
			nil,
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
