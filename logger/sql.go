package logger

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	fmtLogQuery     = `Query: %s`
	fmtLogArgs      = `Args:  %#v`
	fmtLogError     = `Error: %v`
	fmtLogTimeTaken = `Time:  %0.5fs`
)

var reInvisibleChars = regexp.MustCompile(`[\s\r\n\t]+`)

type QueryStatus struct {
	Query string
	Args  interface{}

	Start time.Time
	End   time.Time

	Err error
}

func (q *QueryStatus) String() string {
	lines := make([]string, 0, 8)

	if query := q.Query; query != "" {
		query = reInvisibleChars.ReplaceAllString(query, ` `)
		query = strings.TrimSpace(query)
		lines = append(lines, fmt.Sprintf(fmtLogQuery, query))
	}

	if args, ok := q.Args.([]interface{}); ok && len(args) == 0 {
		q.Args = nil
	}

	if q.Args != nil {
		lines = append(lines, fmt.Sprintf(fmtLogArgs, q.Args))
	}

	if q.Err != nil {
		lines = append(lines, fmt.Sprintf(fmtLogError, q.Err))
	}

	lines = append(lines, fmt.Sprintf(fmtLogTimeTaken, float64(q.End.UnixNano()-q.Start.UnixNano())/float64(1e9)))

	return strings.Join(lines, "\n")
}

func ExplainSQL(m *QueryStatus, show bool) {
	if show {
		Infof("\n\t%s\n\n", strings.Replace(m.String(), "\n", "\n\t", -1))
	}
}
