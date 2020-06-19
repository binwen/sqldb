package clause_test

import (
	"fmt"
	"testing"

	"sqldb/clause"
)

func TestFor(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.For{Locks: []clause.Lock{{Strength: "UPDATE"}}}},
			"SELECT * FROM `user` FOR UPDATE",
			nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.For{Locks: []clause.Lock{
					{Strength: "UPDATE"},
					{Strength: "SHARE", Table: clause.Table{Name: clause.CurrentTable}},
				},
				}},
			"SELECT * FROM `user` FOR UPDATE FOR SHARE OF `user`",
			nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{},
				clause.For{Locks: []clause.Lock{
					{Strength: "UPDATE"},
					{Strength: "SHARE", Table: clause.Table{Name: clause.CurrentTable}},
				},
				},
				clause.For{
					Locks: []clause.Lock{{Strength: "UPDATE", Options: "NOWAIT"}},
				}},
			"SELECT * FROM `user` FOR UPDATE FOR SHARE OF `user` FOR UPDATE NOWAIT",
			nil,
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
