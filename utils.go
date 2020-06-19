package sqldb

import (
	"reflect"
	"regexp"
	"unicode"
)

var (
	inPattern     = regexp.MustCompile(`(?i)\s+in[\s|(]*\?[\s|)]*`)
	insertPattern = regexp.MustCompile(`(?i)\s*insert\s+`)
)

func ConvertInSQL(sql string) string {
	return inPattern.ReplaceAllString(sql, " in (?) ")
}

func IsInsertSQL(sql string) bool {
	return insertPattern.MatchString(sql)
}

func IsChar(c rune) bool {
	if c == '_' || c == '*' {
		return false
	}

	return !unicode.IsLetter(c) && !unicode.IsNumber(c)
}

func IsNil(val reflect.Value) bool {
	if !val.IsValid() {
		return true
	}
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Slice, reflect.Map, reflect.UnsafePointer:
		return val.IsNil()
	case reflect.Ptr:
		if IndirectType(val.Type()).Kind() != reflect.Struct {
			return val.IsNil()
		}
	}
	return false
}

func IndirectType(v reflect.Type) reflect.Type {
	if v.Kind() != reflect.Ptr {
		return v
	}
	return v.Elem()
}

func IsIntZero(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}

	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return value.Uint() == 0
	}
	return false
}
