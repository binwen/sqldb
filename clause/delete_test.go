package clause_test

import (
	"fmt"
	"testing"

	"sqldb/clause"
)

func TestDelete(t *testing.T) {
	results := []struct {
		Clauses []clause.IClause
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.IClause{clause.Delete{}, clause.From{}},
			"DELETE FROM `user`",
			nil,
		},
		{
			[]clause.IClause{clause.Delete{Modifier: "LOW_PRIORITY"}, clause.From{}},
			"DELETE LOW_PRIORITY FROM `user`",
			nil,
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
