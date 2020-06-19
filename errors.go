package sqldb

import "errors"

var (
	ErrRecordNotFound     = errors.New("record not found")
	ErrMissingWhereClause = errors.New("missing WHERE clause while deleting")
)
