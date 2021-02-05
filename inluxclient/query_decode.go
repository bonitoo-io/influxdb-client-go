package influxclient

import (
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Decode decodes the current row into x, which should be
// a pointer to a struct. Columns in the row are decoded into
// appropriate fields in the struct, using the tag conventions
// described by encoding/json to determine how to map
// column names to struct fields.
func (r *QueryResults) Decode(x interface{}) error {
	typeOf := reflect.TypeOf(x)
	xVal := reflect.ValueOf(x)
	if x == nil {
		return errors.New("cannot decode into a nil object")
	}
	kind := typeOf.Kind()
	switch {
	case kind == reflect.Map:
		if typeOf.Key().Kind() != reflect.String {
			return errors.New("cannot marshal into a map where the key is not of a string kind")
		}
		elem := typeOf.Elem()
		// easy case
		if elem.Kind() == reflect.String {
			for i, x := range r.row[1:] {
				val := stringTernary(x, r.columns[i].DefaultValue)
				if val == "" {
					continue
				}
				xVal.SetMapIndex(
					reflect.ValueOf(r.columns[i].Name),
					reflect.ValueOf(val))
			}
			return nil
		}
		for i, x := range r.values {
			if x == nil {
				continue
			}

			if !reflect.TypeOf(x).ConvertibleTo(elem) {
				return fmt.Errorf("cannot marshal type %s into type %s", reflect.TypeOf(x), elem)
			}
			xVal.SetMapIndex(reflect.ValueOf(r.columns[i].Name), reflect.ValueOf(x))
		}
	case kind == reflect.Ptr && typeOf.Elem().Kind() == reflect.Struct:
		xVal = xVal.Elem()
		typeOf = typeOf.Elem()
		numFields := typeOf.NumField()
		for i := 0; i < numFields; i++ {
			f := typeOf.Field(i)
			name := f.Name
			usedTag := false
			if tag, ok := f.Tag.Lookup("flux"); ok {
				if tag != "" {
					name = tag
					usedTag = true
				}
			}
			if _, nameOk := r.names[name]; !nameOk {
				lowerName := strings.ToLower(name)
				if _, lowerNameok := r.names[lowerName]; !lowerNameok && !usedTag {
					continue
				} else if lowerNameok && !usedTag {
					name = lowerName
				}
			}
			fVal := xVal.Field(i)
			// ignore fields that are private or otherwise unsetable
			if !fVal.CanSet() {
				continue
			}
			// grab the row by name
			s := r.row[r.names[name]+1]

			fType := f.Type
			if fType.Kind() == reflect.Ptr {
				if s == "" { // handle nil case
					fVal.Set(reflect.New(fType).Elem())
					continue
				}
				fVal.Set(reflect.New(f.Type.Elem()))
				fType = fVal.Elem().Type()
				for fType.Kind() == reflect.Ptr {
					fVal.Elem().Set(reflect.New(fType.Elem()))
					fVal = fVal.Elem()
					fType = fType.Elem()
				}
				fVal = fVal.Elem()
			}
			switch fType.Kind() {
			case reflect.String:
				fVal.SetString(s)
			case reflect.Bool:
				if r.columns[r.names[name]].DataType != boolDatatype {
					return errors.New("cannot marshal column into a bool type")
				}
				val := false
				if s == "true" {
					val = true
				}
				fVal.SetBool(val)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if s == "" {
					fVal.SetInt(0)
					continue
				}
				switch fType {
				case reflect.TypeOf(time.Duration(0)): //Duration is int64
					val, err := time.ParseDuration(s)
					if err != nil {
						return fmt.Errorf("cannot marshal column value %s into the time.Duration type", s)
					}
					fVal.SetInt(int64(val))
				default:
					val, err := strconv.ParseInt(s, 10, 64)
					if err != nil {
						return fmt.Errorf("cannot marshal column value %s into an int type", s)
					}
					fVal.SetInt(val)
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				if s == "" {
					fVal.SetUint(0)
					continue
				}
				val, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					return errors.New("cannot marshal column into an uint type")
				}
				fVal.SetUint(val)
			case reflect.Float32:
				if s == "" {
					fVal.SetFloat(0)
					continue
				}
				val, err := strconv.ParseFloat(s, 32)
				if err != nil {
					return errors.New("cannot marshal column into an float32 type")
				}
				fVal.SetFloat(val)

			case reflect.Float64:
				if s == "" {
					fVal.SetFloat(0)
					continue
				}
				val, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return errors.New("cannot marshal column into an float64 type")
				}
				xVal.Field(i).SetFloat(val)
			case reflect.Struct:
				if fType != reflect.TypeOf(time.Time{}) {
					return errors.New("the only struct supported is a time.Time")
				}
				ts, err := time.Parse(time.RFC3339, s)
				if err != nil {
					return errors.New("cannot marshal column into a time")
				}
				fVal.Set(reflect.ValueOf(ts))
			case reflect.Interface:
				if s == "" {
					fVal.Set(reflect.Zero(fType))
					break
				}
				x := r.ValueByName(name)
				if !reflect.TypeOf(x).ConvertibleTo(f.Type) {
					return fmt.Errorf("cannot convert type column to type %s", f.Type)
				}
				fVal.Set(reflect.ValueOf(x))
			case reflect.Array, reflect.Slice:
				if r.columns[r.names[name]].DataType != base64BinaryDataType {
					return errors.New("cannot marshal column into a slice type")
				}
				elem := fType.Elem()
				if elem.Kind() != reflect.Uint8 {
					return fmt.Errorf("cannot marshal column into a slice type %s", elem)
				}
				val, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					return fmt.Errorf("cannot decode column into a time: %w", err)
				}
				fVal.SetBytes(val)
			}

		}
	case kind == reflect.Struct:
		return errors.New("struct argument must be a pointer")
	default:
		return fmt.Errorf("cannot marshal into a type of %s", typeOf)
	}
	return nil
}

func stringTernary(a, b string) string {
	if a == "" {
		return b
	}
	return a
}
