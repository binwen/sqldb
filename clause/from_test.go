package clause_test

import (
	"fmt"
	"testing"

	"github.com/binwen/sqldb/clause"
)

func TestFrom(t *testing.T) {
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
			[]clause.IClause{
				clause.Select{}, clause.From{
					Tables: []clause.Table{{Name: "user"}},
					Joins: []clause.Join{
						{
							Type:  clause.InnerJoin,
							Table: clause.Table{Name: "books"},
							ON: clause.Where{
								Exprs: []clause.Expression{
									clause.EQ{
										Column: clause.Column{Table: "books", Name: "id"},
										Value:  clause.Column{Table: "user", Name: "id"},
									},
								},
							},
						},
					},
				},
			},
			"SELECT * FROM `user` INNER JOIN `books` ON `books`.`id` = `user`.`id`", nil,
		},
		{
			[]clause.IClause{
				clause.Select{},
				clause.From{
					Tables: []clause.Table{{Name: "user"}},
					Joins: []clause.Join{
						{
							Type:  clause.InnerJoin,
							Table: clause.Table{Name: "articles"},
							ON: clause.Where{
								Exprs: []clause.Expression{
									clause.EQ{
										Column: clause.Column{Table: "articles", Name: "id"},
										Value:  clause.Column{Table: "user", Name: "id"}},
								},
							},
						},
						{
							Type:  clause.LeftJoin,
							Table: clause.Table{Name: "companies"},
							Using: []string{"company_name"},
						},
					},
				},
				clause.From{
					Joins: []clause.Join{
						{
							Type:  clause.RightJoin,
							Table: clause.Table{Name: "profiles"},
							ON: clause.Where{
								Exprs: []clause.Expression{
									clause.EQ{
										Column: clause.Column{Table: "profiles", Name: "email"},
										Value:  clause.Column{Table: clause.CurrentTable, Name: "email"},
									},
								},
							},
						},
					},
				},
			},
			"SELECT * FROM `user` INNER JOIN `articles` ON `articles`.`id` = `user`.`id` LEFT JOIN `companies` USING (`company_name`) RIGHT JOIN `profiles` ON `profiles`.`email` = `user`.`email`",
			nil,
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			checkBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
