package sqldb

import (
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
)

func MakeMapScan(rows *sqlx.Rows) (map[string]interface{}, error) {
	mapping := map[string]interface{}{}
	err := ParseMapScan(rows, mapping)
	if err != nil {
		return mapping, err
	}
	return mapping, nil
}

func ParseMapScan(rows *sqlx.Rows, mapValue map[string]interface{}) error {
	err := rows.MapScan(mapValue)
	if err != nil {
		return err
	}
	for k, v := range mapValue {
		switch v.(type) {
		case []uint8:
			v = string(v.([]uint8))
		case nil:
			v = ""
		}
		mapValue[k] = v
	}
	return nil
}

func ScanAll(rows *sqlx.Rows, dest DestWrapper) error {
	switch values := dest.Dest.(type) {
	case map[string]interface{}, *map[string]interface{}:
		mapValue, ok := values.(map[string]interface{})
		if !ok {
			if v, ok := values.(*map[string]interface{}); ok {
				mapValue = *v
			}
		}

		if mapValue == nil {
			return fmt.Errorf("nil pointer passed to scan destination, gov `%v`", mapValue)
		}

		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return ErrRecordNotFound
		}

		err := ParseMapScan(rows, mapValue)
		if err != nil {
			return err
		}
	case *[]map[string]interface{}:
		for rows.Next() {
			mapping, err := MakeMapScan(rows)
			if err != nil {
				return err
			}
			*values = append(*values, mapping)
		}
	case *[]*map[string]interface{}:
		for rows.Next() {
			mapping, err := MakeMapScan(rows)
			if err != nil {
				return err
			}
			*values = append(*values, &mapping)
		}
	default:
		//direct := reflect.Indirect(reflect.ValueOf(dest))
		kind := dest.ReflectValue.Kind()
		switch kind {
		case reflect.Slice, reflect.Array:
			directType := dest.ReflectValue.Type().Elem()
			isPtr := directType.Kind() == reflect.Ptr
			for directType.Kind() == reflect.Ptr {
				directType = directType.Elem()
			}

			directKind := directType.Kind()
			for rows.Next() {
				out := reflect.New(directType)
				switch directKind {
				case reflect.Struct:
					err := rows.StructScan(out.Interface())
					if err != nil {
						return err
					}
				default:
					err := rows.Scan(out.Interface())
					if err != nil {
						return err
					}
				}
				if isPtr {
					dest.ReflectValue.Set(reflect.Append(dest.ReflectValue, out))
				} else {
					dest.ReflectValue.Set(reflect.Append(dest.ReflectValue, reflect.Indirect(out)))
				}
			}
		case reflect.Struct:
			if !rows.Next() {
				if err := rows.Err(); err != nil {
					return err
				}
				return ErrRecordNotFound
			}
			return rows.StructScan(dest.Dest)
		case reflect.Ptr:
			if !rows.Next() {
				if err := rows.Err(); err != nil {
					return err
				}
				return ErrRecordNotFound
			}

			directType := dest.ReflectValue.Type().Elem()
			out := reflect.New(directType).Elem().Addr()

			switch directType.Kind() {
			case reflect.Struct:
				err := rows.StructScan(out.Interface())
				if err != nil {
					return err
				}
				dest.ReflectValue.Set(out)
			case reflect.Map:
				mapping, err := MakeMapScan(rows)
				if err != nil {
					return err
				}
				for k, v := range mapping {
					dest.ReflectValue.Elem().SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(v))
				}
			}
		default:
			if !rows.Next() {
				if err := rows.Err(); err != nil {
					return err
				}
				return ErrRecordNotFound
			}
			return rows.Scan(dest.Dest)
		}
	}
	return rows.Close()
}
