package sqldb

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx"

	"github.com/binwen/sqldb/clause"
)

var mapper = NewReflectMapperFunc("db", strings.ToLower)

type Session struct {
	Error     error
	db        *SqlDB
	statement *Statement
}

type DestWrapper struct {
	Dest         interface{}
	ReflectValue reflect.Value
}

func Expr(expr string, args ...interface{}) *clause.Expr {
	return &clause.Expr{SQL: expr, Vars: args}
}

func NewSession(db *SqlDB, table string) *Session {
	var tables []clause.Table

	for _, t := range strings.Split(table, ",") {
		ts := strings.FieldsFunc(t, IsChar)
		size := len(ts)

		if size == 1 {
			tables = append(tables, clause.Table{Name: ts[0]})
		} else if size == 3 && strings.ToUpper(ts[1]) == "AS" {
			tables = append(tables, clause.Table{Name: ts[0], Alias: ts[2]})
		} else {
			tables = append(tables, clause.Table{Name: t, Raw: true})
		}
	}
	session := &Session{
		db: db,
		statement: &Statement{
			Dialector: db.engine.Dialector,
			Clauses:   map[string]clause.Clause{},
			Tables:    tables,
		},
	}

	return session
}

func (session *Session) AddError(err error) {
	if session.Error == nil {
		session.Error = err
	} else if err != nil {
		session.Error = fmt.Errorf("%v; %w", session.Error, err)
	}
	return
}

func (session *Session) Select(columns ...string) *Session {
	if columns == nil {
		session.statement.AddClause(clause.Select{})
		return session
	}
	clauseSelect := clause.Select{}

	for _, column := range columns {
		columnGroups := strings.Split(column, ",")
		for _, col := range columnGroups {
			fields := strings.FieldsFunc(col, IsChar)
			size := len(fields)
			if size == 1 {
				clauseSelect.Columns = append(clauseSelect.Columns, clause.Column{Name: fields[0]})
			} else if size == 3 && strings.ToUpper(fields[1]) == "AS" {
				clauseSelect.Columns = append(clauseSelect.Columns, clause.Column{Name: fields[0], Alias: fields[2]})
			} else {
				clauseSelect.Columns = append(clauseSelect.Columns, clause.Column{Name: col, Raw: true})
			}
		}
	}
	session.statement.AddClause(clauseSelect)
	return session
}

func (session *Session) SelectExpr(query string, args ...interface{}) *Session {
	session.statement.AddClause(clause.Select{Expressions: []clause.Expression{clause.Expr{SQL: query, Vars: args}}})
	return session
}

func (session *Session) Distinct(columns ...string) *Session {
	session.statement.AddClause(clause.Select{Distinct: true})
	return session.Select(columns...)
}

func (session *Session) Limit(limit int) *Session {
	session.statement.AddClause(clause.Limit{Limit: limit})
	return session
}

func (session *Session) Offset(offset int) *Session {
	session.statement.AddClause(clause.Limit{Offset: offset})
	return session
}

func (session *Session) GroupBy(name string) *Session {
	session.statement.AddClause(clause.GroupBy{Columns: []clause.Column{{Name: name}}})
	return session
}

func (session *Session) Having(query interface{}, args ...interface{}) *Session {
	conditions, err := session.statement.BuildCondition(query, args...)
	if err != nil {
		session.AddError(err)
	} else {
		session.statement.AddClause(clause.GroupBy{Having: conditions})
	}

	return session
}

func (session *Session) OrderBy(order string) *Session {
	session.statement.AddClause(clause.OrderBy{
		Columns: []clause.OrderByColumn{{Column: clause.Column{Name: order, Raw: true}}},
	})

	return session
}

// 降序字段
func (session *Session) Desc(columns ...string) *Session {
	order := clause.OrderBy{}
	for _, column := range columns {
		order.Columns = append(order.Columns, clause.OrderByColumn{Column: clause.Column{Name: column}, Desc: true})
	}
	session.statement.AddClause(order)
	return session
}

// 升序字段
func (session *Session) Asc(columns ...string) *Session {
	order := clause.OrderBy{}
	for _, column := range columns {
		order.Columns = append(order.Columns, clause.OrderByColumn{Column: clause.Column{Name: column}, Desc: false})
	}
	session.statement.AddClause(order)
	return session
}

func (session *Session) Where(query interface{}, args ...interface{}) *Session {
	conditions, err := session.statement.BuildCondition(query, args...)
	if err != nil {
		session.AddError(err)
	} else if len(conditions) > 0 {
		session.statement.AddClause(clause.Where{Exprs: conditions})
	}
	return session
}

func (session *Session) Not(query interface{}, args ...interface{}) *Session {
	conditions, err := session.statement.BuildCondition(query, args...)
	if err != nil {
		session.AddError(err)
	} else if len(conditions) > 0 {
		session.statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.Not(conditions...)}})
	}

	return session
}

func (session *Session) Or(query interface{}, args ...interface{}) *Session {
	conditions, err := session.statement.BuildCondition(query, args...)
	if err != nil {
		session.AddError(err)
	} else if len(conditions) > 0 {
		session.statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.Or(conditions...)}})
	}

	return session
}

func (session *Session) Join(condition string, args ...interface{}) *Session {
	session.statement.AddClause(clause.From{
		Joins: []clause.Join{{Expression: clause.Expr{SQL: condition, Vars: args}}},
	})
	return session
}

func (session *Session) buildQuerySQL() {
	session.statement.SQL.Grow(100)
	if f, ok := session.statement.Clauses["FROM"].Expression.(clause.From); !ok || len(f.Tables) == 0 {
		session.statement.AddClause(clause.From{Tables: session.statement.Tables})
	}

	session.statement.AddClauseIfNotExists(clause.Select{})
	session.statement.Build("HINT", "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "FOR")
}

func (session *Session) execQuery(dest DestWrapper) (err error) {
	if session.Error != nil {
		return session.Error
	}

	if session.statement.SQL.String() == "" {
		session.buildQuerySQL()
	}

	rows, err := session.db.Query(session.statement.SQL.String(), session.statement.SQLVars...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return ScanAll(rows, dest)
}

func (session *Session) Find(dest interface{}) error {
	defer session.Clear()
	destRefValue := reflect.ValueOf(dest)
	if destRefValue.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to scan destination")
	}
	destWrapper := DestWrapper{Dest: dest, ReflectValue: reflect.Indirect(destRefValue)}

	return session.execQuery(destWrapper)
}

func (session *Session) First(dest interface{}) error {
	defer session.Clear()
	destRefValue := reflect.Indirect(reflect.ValueOf(dest))
	if IsNil(destRefValue) {
		return fmt.Errorf("nil pointer passed to scan destination, gov `%v`", dest)
	}
	destWrapper := DestWrapper{Dest: dest, ReflectValue: destRefValue}
	session.Limit(1)

	return session.execQuery(destWrapper)
}

func (session *Session) Count() (count int64, err error) {
	defer session.Clear()
	if session.Error != nil {
		return count, session.Error
	}

	if s, ok := session.statement.Clauses["SELECT"].Expression.(clause.Select); !ok || len(s.Columns) == 0 {
		session.statement.AddClause(clause.Select{Expressions: []clause.Expression{clause.Expr{SQL: "count(*)"}}})
	}

	if session.statement.SQL.String() == "" {
		session.buildQuerySQL()
	}

	r := session.db.QueryRow(session.statement.SQL.String(), session.statement.SQLVars...)
	err = r.Scan(&count)
	return count, err
}

func (session *Session) Exist() (bool, error) {
	defer session.Clear()
	count, err := session.Count()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func convertCreateValues(dataRefValue reflect.Value, data interface{}) (values clause.Values) {
	switch data.(type) {
	case map[string]interface{}, *map[string]interface{}:
		values.Values = make([][]interface{}, 1)
		mapData, ok := data.(map[string]interface{})
		if !ok {
			if v, ok := data.(*map[string]interface{}); ok {
				mapData = *v
			}
		}
		for k, v := range mapData {
			values.Columns = append(values.Columns, clause.Column{Name: k})
			values.Values[0] = append(values.Values[0], v)
		}
	case []map[string]interface{}, *[]map[string]interface{}:
		var (
			columns       []string
			columnDataMap = map[string][]interface{}{}
		)
		mapDataList, ok := data.([]map[string]interface{})
		if !ok {
			if v, ok := data.(*[]map[string]interface{}); ok {
				mapDataList = *v
			}
		}
		dataLen := len(mapDataList)
		for idx, mapValue := range mapDataList {
			for k, v := range mapValue {
				if _, ok := columnDataMap[k]; !ok {
					columnDataMap[k] = make([]interface{}, dataLen)
					columns = append(columns, k)
				}
				columnDataMap[k][idx] = v
			}
		}
		sort.Strings(columns)
		values.Values = make([][]interface{}, dataLen)
		for _, column := range columns {
			values.Columns = append(values.Columns, clause.Column{Name: column})
			for i, v := range columnDataMap[column] {
				values.Values[i] = append(values.Values[i], v)
			}
		}
	default:
		switch dataRefValue.Kind() {
		case reflect.Slice, reflect.Array:
			dataLen := dataRefValue.Len()
			if dataLen == 0 {
				return
			}

			var (
				columns       []string
				columnDataMap = map[string][]interface{}{}
			)

			directType := dataRefValue.Type().Elem()
			for directType.Kind() == reflect.Ptr {
				directType = directType.Elem()
			}

			switch directType.Kind() {
			case reflect.Struct:
				for i := 0; i < dataLen; i++ {
					for field, value := range mapper.FieldMap(dataRefValue.Index(i)) {
						if IsIntZero(value) {
							continue
						}
						if _, ok := columnDataMap[field]; !ok {
							columnDataMap[field] = make([]interface{}, dataLen)
							columns = append(columns, field)
						}
						columnDataMap[field][i] = reflect.Indirect(value).Interface()
					}
				}
			case reflect.Map:
				for i := 0; i < dataLen; i++ {
					iter := reflect.Indirect(dataRefValue.Index(i)).MapRange()
					for iter.Next() {
						column := iter.Key().String()
						if _, ok := columnDataMap[column]; !ok {
							columnDataMap[column] = make([]interface{}, dataLen)
							columns = append(columns, column)
						}
						columnDataMap[column][i] = reflect.Indirect(iter.Value()).Interface()
					}
				}
			}
			sort.Strings(columns)
			values.Values = make([][]interface{}, dataLen)
			for _, column := range columns {
				values.Columns = append(values.Columns, clause.Column{Name: column})
				for i, v := range columnDataMap[column] {
					values.Values[i] = append(values.Values[i], v)
				}
			}
		case reflect.Struct:
			fields := mapper.FieldMap(dataRefValue)
			values.Values = make([][]interface{}, 1)
			for k, v := range fields {
				if IsIntZero(v) {
					continue
				}
				values.Columns = append(values.Columns, clause.Column{Name: k})
				values.Values[0] = append(values.Values[0], reflect.Indirect(v).Interface())
			}
		}
	}
	return
}

type ExecResult struct {
	err      error
	isId     bool
	idList   []int64
	affected int64
}

func (session *Session) insert(isBulk bool, dataRefValue reflect.Value, data interface{}) *ExecResult {
	session.statement.AddClauseIfNotExists(clause.Insert{Table: clause.Table{Name: session.statement.Tables[0].Name}})
	session.statement.AddClause(convertCreateValues(dataRefValue, data))
	var hasReturning bool
	if session.statement.Dialector.WithReturning() {
		if s, ok := session.statement.Clauses["RETURNING"].Expression.(clause.Select); !ok || len(s.Columns) == 0 {
			session.statement.Dialector.SetQueryer(session.db)
			pkColumnNames := session.statement.Dialector.PKColumnNames(session.statement.Tables[0].Name)
			if pkColumnNames != nil && len(pkColumnNames) == 1 {
				session.statement.AddClause(clause.Returning{Columns: []clause.Column{{Name: pkColumnNames[0]}}})
				hasReturning = true
			} else {
				hasReturning = false
			}
		} else {
			hasReturning = true
		}
	}

	session.statement.Build("INSERT", "VALUES", "ON_CONFLICT", "RETURNING")

	if hasReturning {
		rows, err := session.db.Query(session.statement.SQL.String(), session.statement.SQLVars...)
		if err != nil {
			return &ExecResult{err: err}
		}

		defer rows.Close()
		var idList []int64
		destWrapper := DestWrapper{Dest: &idList, ReflectValue: reflect.Indirect(reflect.ValueOf(&idList))}
		err = ScanAll(rows, destWrapper)
		if err != nil {
			return &ExecResult{err: err}
		}
		return &ExecResult{isId: true, idList: idList}
	}

	result, err := session.db.Exec(session.statement.SQL.String(), session.statement.SQLVars...)
	if err != nil {
		return &ExecResult{err: err}
	}
	lastInsertId, err := result.LastInsertId()
	if err == nil {
		var dataLen int
		if isBulk {
			dataLen = dataRefValue.Len()
		} else {
			dataLen = 1
		}
		var lastInsertIdList []int64
		if session.statement.Dialector.LastInsertIDReversed() {
			for i := dataLen - 1; i >= 0; i-- {
				lastInsertIdList = append(lastInsertIdList, lastInsertId-int64(i))
			}
		} else {
			for i := 0; i < dataLen; i++ {
				lastInsertIdList = append(lastInsertIdList, lastInsertId+int64(i))
			}
		}
		return &ExecResult{isId: true, err: err, idList: lastInsertIdList}
	}

	affected, err := result.RowsAffected()
	return &ExecResult{isId: false, err: err, affected: affected}
}

// 单一创建，返回表自增ID; 值可以map或struct
func (session *Session) Create(data interface{}) (lastInsertId int64, err error) {
	defer session.Clear()
	direct := reflect.Indirect(reflect.ValueOf(data))
	vt := direct.Kind()
	if vt != reflect.Struct && vt != reflect.Map {
		return 0, fmt.Errorf("create an object using the given value must `map` or `struct` structure, got %v", vt)
	}
	result := session.insert(false, direct, data)
	if result.err != nil {
		return 0, err
	}
	if result.isId {
		return result.idList[0], nil
	}
	return 0, nil

}

// 批量创建，返回表自增ID列表; 值可以map或struct组成的slice或array
func (session *Session) BulkCreate(data interface{}) (lastInsertIdList []int64, err error) {
	defer session.Clear()
	direct := reflect.Indirect(reflect.ValueOf(data))
	vt := direct.Kind()
	if vt != reflect.Slice && vt != reflect.Array {
		return lastInsertIdList, fmt.Errorf("bulk create object using the given value must `slice` or `array` structure of `map` or `struct`, got %v", vt)
	}
	if direct.Len() == 0 {
		return lastInsertIdList, fmt.Errorf("bulk create object using the given value cannot empty")
	}

	result := session.insert(true, direct, data)
	if result.err != nil {
		return result.idList, err
	}
	if result.isId {
		return result.idList, nil
	}
	return
}

// 修改单一字段，返回受影响的行数
func (session *Session) Update(column string, value interface{}) (affected int64, err error) {
	defer session.Clear()
	session.statement.AddClauseIfNotExists(clause.Update{Table: session.statement.Tables[0]})
	session.statement.AddClause(clause.Set{Assignments: []clause.Assignment{{clause.Column{Name: column}, value}}})
	session.statement.Build("UPDATE", "SET", "WHERE")
	result, err := session.db.Exec(session.statement.SQL.String(), session.statement.SQLVars...)
	if err != nil {
		return
	}
	return result.RowsAffected()
}

// 批量修改多个字段，返回受影响的行数
func (session *Session) BulkUpdate(data map[string]interface{}) (affected int64, err error) {
	defer session.Clear()
	var sets = make([]clause.Assignment, 0, len(data))
	var keys []string
	for k, _ := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sets = append(sets, clause.Assignment{
			Column: clause.Column{Name: k},
			Value:  data[k],
		})
	}
	session.statement.AddClauseIfNotExists(clause.Update{Table: session.statement.Tables[0]})
	session.statement.AddClause(clause.Set{Assignments: sets})
	session.statement.Build("UPDATE", "SET", "WHERE")
	result, err := session.db.Exec(session.statement.SQL.String(), session.statement.SQLVars...)
	if err != nil {
		return
	}
	return result.RowsAffected()
}

// 删除，必须要where条件，返回受影响的行数
func (session *Session) Delete() (affected int64, err error) {
	defer session.Clear()
	if session.statement.SQL.String() == "" {
		if _, ok := session.statement.Clauses["WHERE"]; !ok {
			return 0, ErrMissingWhereClause
		}
		session.statement.AddClauseIfNotExists(clause.Delete{})
		session.statement.AddClauseIfNotExists(clause.From{Tables: session.statement.Tables})
		session.statement.Build("DELETE", "FROM", "WHERE")
	}
	result, err := session.db.Exec(session.statement.SQL.String(), session.statement.SQLVars...)
	if err != nil {
		return
	}
	return result.RowsAffected()
}

func (session *Session) Query() (*sqlx.Rows, error) {
	defer session.Clear()
	if session.Error != nil {
		return nil, session.Error
	}

	if session.statement.SQL.String() == "" {
		session.buildQuerySQL()
	}

	return session.db.Query(session.statement.SQL.String(), session.statement.SQLVars...)
}

func (session *Session) QueryRow() *sqlx.Row {
	defer session.Clear()
	if session.statement.SQL.String() == "" {
		session.buildQuerySQL()
	}

	return session.db.QueryRow(session.statement.SQL.String(), session.statement.SQLVars...)
}

func (session *Session) Hint(query string) *Session {
	session.statement.Hint = query
	return session
}

func (session *Session) Master() *Session {
	session.db.isMaster = true
	return session
}

func (session *Session) Clear() {
	session.statement.ReInit()
}
