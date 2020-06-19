package sqldb

import (
	"reflect"

	"github.com/jmoiron/sqlx/reflectx"
)

type ReflectMapper struct {
	mapper *reflectx.Mapper
}

func NewReflectMapper(tagName string) *ReflectMapper {
	return &ReflectMapper{
		mapper: reflectx.NewMapper(tagName),
	}
}

func NewReflectMapperFunc(tagName string, f func(string) string) *ReflectMapper {
	return &ReflectMapper{
		mapper: reflectx.NewMapperFunc(tagName, f),
	}
}

func (r *ReflectMapper) FieldByName(v reflect.Value, name string) reflect.Value {
	return r.mapper.FieldByName(v, name)
}

func (r *ReflectMapper) FieldMap(v reflect.Value) map[string]reflect.Value {
	v = reflect.Indirect(v)

	ret := map[string]reflect.Value{}
	tm := r.mapper.TypeMap(v.Type())
	for tagName, fi := range tm.Names {
		if (fi.Parent.Zero.Kind() == reflect.Struct || (fi.Zero.Kind() == reflect.Ptr && fi.Zero.Type().Elem().Kind() == reflect.Struct)) && !fi.Parent.Field.Anonymous {
			continue
		}
		ret[tagName] = reflectx.FieldByIndexes(v, fi.Index)
	}

	return ret
}
