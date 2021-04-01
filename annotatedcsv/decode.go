package annotatedcsv

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"time"

	ireflect "github.com/influxdata/influxdb-client-go/internal/reflect"
)

// Decode decodes the current row into x, which should be
// a pointer to a struct or a pointer to a slice.
//
// When it's a pointer to a struct, columns in the row are decoded into
// appropriate fields in the struct, using the similar tag conventions
// described by encoding/json to determine how to map
// column names to struct fields. Tag prefix must start with "flux:":
//
//  type Point struct {
//      Timestamp  time.Time `flux:"_time"`
//      Value      float32   `flux:"_value"`
//      Location   string    `flux:"location"`
//      Sensor     string    `flux:"type"`
//  }
//
//  var p Point
//  err := r.Decode(&p)
//
// When it's a pointer to a slice, the slice is changed
// to have one element for each column (reusing
// space in the slice if possible), and each element is
// set to the value in the corresponding column.
//
// When decoding into an empty interface value, the resulting
// type depends on the column type:
//
// - string, tag or unrecognized: string
// - double: float64
// - unsignedLong: uint64
// - long: int64
// - boolean: bool
// - duration: time.Duration
// - dateTime: time.Time
//
// Any value can be decoded into a string without
// error - the result is the value in the CSV, so
//
//     var row []string
//     r.Decode(&row)
//
// will always succeed and provide all the values in the column as strings.
func (r *Reader) Decode(x interface{}) error {
	t := reflect.TypeOf(x)
	if err := r.initColumns(t, r.cols); err != nil {
		return err
	}
	et := t.Elem()
	switch et.Kind() {
	case reflect.Struct:
		v := reflect.ValueOf(x).Elem()
		err := forEachField(et, func(f reflect.StructField, name string) error {
			i, ok := r.columnIndexes[name]
			if ok {
				if err := r.convertColumnValue(v.FieldByIndex(f.Index), i); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	case reflect.Slice:
		v := reflect.ValueOf(x)
		e := v.Elem()
		c := len(r.cols)
		if e.IsNil() || e.Len() < c {
			e = reflect.MakeSlice(e.Type(), c, c)
		}
		for i := 0; i < c; i++ {
			if err := r.convertColumnValue(e.Index(i), i); err != nil {
				return err
			}
		}
		v.Elem().Set(e)
	}
	return nil
}

// initColumns initializes r.colSetters for the given columns.
// It's called once per section.
func (r *Reader) initColumns(t reflect.Type, columns []Column) error {
	if t == r.decodeType {
		return nil
	}
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("cannot decode into non-pointer type")
	}
	et := t.Elem()
	if et.Kind() != reflect.Struct && et.Kind() != reflect.Slice {
		return fmt.Errorf("can decode into pointer to %v", et)
	}
	var f fieldTypeOf
	switch et.Kind() {
	case reflect.Struct:
		fieldsMap := make(map[string]reflect.StructField)
		err := forEachField(et, func(f reflect.StructField, name string) error {
			fieldsMap[name] = f
			return nil
		})
		if err != nil {
			return err
		}

		f = func(col Column) reflect.Type {
			if f, ok := fieldsMap[col.Name]; ok {
				return f.Type
			}
			return nil
		}
	case reflect.Slice:
		s := et.Elem()
		if s.Kind() != reflect.String && s.Kind() != reflect.Interface {
			return fmt.Errorf("cannot decode into pointer to slice of %v", s)
		}
		f = func(col Column) reflect.Type {
			return s
		}
	}
	setters, err := fieldSetters(columns, f)
	if err != nil {
		return err
	}
	r.colSetters = setters
	r.decodeType = t

	return nil
}

// convertColumnValue set a value from current row of given column index
// to a struct or a slice field value
func (r *Reader) convertColumnValue(v reflect.Value, colIndex int) error {
	s := stringTernary(r.row[colIndex], r.cols[colIndex].Default)
	if err := r.colSetters[colIndex](v, s); err != nil {
		return fmt.Errorf(`cannot convert value "%s" to type "%s" at line %d: %w`, s, r.cols[colIndex].Type, r.r.Line(), err)
	}
	return nil
}

type fieldFunc func(f reflect.StructField, name string) error

// forEachField enumerates visible fields of t, finds field name and calls field function
func forEachField(t reflect.Type, ff fieldFunc) error {
	fields := ireflect.VisibleFields(t)
	for _, f := range fields {
		name := f.Name
		if tag, ok := f.Tag.Lookup("flux"); ok {
			if tag == "-" {
				continue
			}
			name = tag
		}
		if err := ff(f, name); err != nil {
			return err
		}
	}
	return nil
}

// fieldKind represents one of the possible kinds of struct field.
// It's similar to reflect.Kind (every reflect.Kind constant i
// is represented as fieldKind(i)) except that it also has
// defined constants for field types that have special treatment,
// such as time.Duration.
type fieldKind uint

// conv represents a possible conversion source and destination.
type conv struct {
	from colType
	to   fieldKind
}

// intKinds holds all supported signed integer kinds
var intKinds = []fieldKind{
	fieldKind(reflect.Int),
	fieldKind(reflect.Int8),
	fieldKind(reflect.Int16),
	fieldKind(reflect.Int32),
	fieldKind(reflect.Int64),
}

// uintKinds holds all supported unsigned integer kinds
var uintKinds = []fieldKind{
	fieldKind(reflect.Uint),
	fieldKind(reflect.Uint8),
	fieldKind(reflect.Uint16),
	fieldKind(reflect.Uint32),
	fieldKind(reflect.Uint64),
}

// floatKinds holds all supported floating point number kinds
var floatKinds = []fieldKind{
	fieldKind(reflect.Float32),
	fieldKind(reflect.Float64),
}

// rest of supported type kinds
const (
	durationKind = fieldKind(255 + iota)
	timeKind
	bytesKind
)

// colType is integer based representation annotated CSV types
type colType uint

const (
	stringCol = colType(iota)
	boolCol
	durationCol
	longCol
	uLongCol
	doubleCol
	base64BinaryCol
	timeColRFC
	timeColRFCNano
)

// canonicalTypes holds the Go type that best represents
// all the annotated CSV column kinds (keyed  by column type).
var canonicalTypes = []reflect.Type{
	stringCol:       reflect.TypeOf(""),
	boolCol:         reflect.TypeOf(true),
	durationCol:     reflect.TypeOf(time.Duration(1)),
	longCol:         reflect.TypeOf(int64(0)),
	uLongCol:        reflect.TypeOf(uint64(0)),
	doubleCol:       reflect.TypeOf(0.0),
	base64BinaryCol: reflect.TypeOf([]byte{}),
	timeColRFC:      reflect.TypeOf(time.Time{}),
	timeColRFCNano:  reflect.TypeOf(time.Time{}),
}

// columnTypes maps annotated CSV types
// to integer column type
var columnTypes = map[string]colType{
	"string":               stringCol,
	"boolean":              boolCol,
	"duration":             durationCol,
	"long":                 longCol,
	"unsignedLong":         uLongCol,
	"double":               doubleCol,
	"base64Binary":         base64BinaryCol,
	"dateTime:RFC3339":     timeColRFC,
	"dateTime:RFC3339Nano": timeColRFCNano,
	"dateTime":             timeColRFCNano,
}

// conversions maps all possible conversions from column types to field kinds
var conversions map[conv]fieldSetter

// fieldKindOf determines a fieldKind for given reflect.Type
func fieldKindOf(t reflect.Type) fieldKind {
	switch t {
	case canonicalTypes[durationCol]:
		return durationKind
	case canonicalTypes[timeColRFC], canonicalTypes[timeColRFCNano]:
		return timeKind
	case canonicalTypes[base64BinaryCol]:
		if t.Elem().Kind() == reflect.Uint8 {
			return bytesKind
		}
	}
	return fieldKind(t.Kind())
}

type fieldSetter = func(v reflect.Value, s string) error

type fieldTypeOf func(col Column) reflect.Type

// fieldSetters returns slice of functions for converting
// appropriate column values to type of a struct or a slice field
func fieldSetters(columns []Column, f fieldTypeOf) ([]fieldSetter, error) {
	setters := make([]fieldSetter, len(columns))
	for i, col := range columns {
		ftype := f(col)
		if ftype == nil {
			continue
		}
		colType, ok := columnTypes[col.Type]
		if !ok {
			// ignore invalid type and use string
			colType = stringCol
		}
		fkind := fieldKindOf(ftype)
		convert, ok := conversions[conv{colType, fkind}]
		if !ok {
			return nil, fmt.Errorf("cannot convert from column type %s to %v", col.Type, ftype)
		}
		setters[i] = convert
	}
	return setters, nil
}

// toInterface returns a function for setting value of given column type
// to an interface{} field
func toInterface(col colType) fieldSetter {
	t := canonicalTypes[col]
	convert, ok := conversions[conv{col, fieldKindOf(t)}]
	if !ok {
		panic("conversion not found (should not happen)")
	}
	return func(v reflect.Value, s string) error {
		e := reflect.New(t).Elem()
		if err := convert(e, s); err != nil {
			return err
		}
		v.Set(e)
		return nil
	}
}

// toString sets a string to a value of type string
func toString(v reflect.Value, s string) error {
	v.SetString(s)
	return nil
}

// toFloat converts a string to a value of type float
func toFloat(v reflect.Value, s string) error {
	x, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	if v.OverflowFloat(x) {
		return fmt.Errorf("overflow")
	}
	v.SetFloat(x)
	return nil
}

// toBool converts a string to a value of type bool
func toBool(v reflect.Value, s string) error {
	var b bool
	switch s {
	case "true":
		b = true
	case "false":
		b = false
	default:
		return fmt.Errorf("invalid bool value")
	}
	v.SetBool(b)
	return nil
}

// toInt converts a string to a value of type signed int
func toInt(v reflect.Value, s string) error {
	x, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	if v.OverflowInt(x) {
		return fmt.Errorf("overflow")
	}
	v.SetInt(x)
	return nil
}

// toUint converts a string to a value of type unsigned int
func toUint(v reflect.Value, s string) error {
	x, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return err
	}
	if v.OverflowUint(x) {
		return fmt.Errorf("overflow")
	}
	v.SetUint(x)
	return nil
}

// toTime converts a string to a time value
func toTime(v reflect.Value, s string) error {
	x, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(x))
	return nil
}

// toDuration converts a string to a duration value
func toDuration(v reflect.Value, s string) error {
	x, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(x))
	return nil
}

// toBytes decodes a base64 encoded string to a slice of bytes
func toBytes(v reflect.Value, s string) error {
	x, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(x))
	return nil
}

// stringTernary returns second argument of first is empty, otherwise first
func stringTernary(s string, d string) string {
	if s != "" {
		return s
	}
	return d
}

func init() {
	conversions = make(map[conv]fieldSetter)
	for _, k := range intKinds {
		conversions[conv{longCol, k}] = toInt
		conversions[conv{uLongCol, k}] = toInt
	}
	for _, k := range uintKinds {
		conversions[conv{uLongCol, k}] = toUint
	}
	for _, k := range floatKinds {
		conversions[conv{longCol, k}] = toFloat
		conversions[conv{uLongCol, k}] = toFloat
		conversions[conv{doubleCol, k}] = toFloat
	}
	conversions[conv{boolCol, fieldKind(reflect.Bool)}] = toBool
	conversions[conv{stringCol, fieldKind(reflect.String)}] = toString
	conversions[conv{durationCol, durationKind}] = toDuration
	conversions[conv{base64BinaryCol, bytesKind}] = toBytes
	conversions[conv{timeColRFC, timeKind}] = toTime
	conversions[conv{timeColRFCNano, timeKind}] = toTime

	for t := range canonicalTypes {
		conversions[conv{colType(t), fieldKind(reflect.Interface)}] = toInterface(colType(t))
		conversions[conv{colType(t), fieldKind(reflect.String)}] = toString
	}
}
